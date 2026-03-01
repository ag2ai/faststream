import asyncio
import contextlib
import logging
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
)
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any, Optional, TypeVar, cast

from typing_extensions import override

from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream.exceptions import FeatureNotSupportedException
from faststream.sqla.client import SqlaBaseClient
from faststream.sqla.message import SqlaInnerMessage
from faststream.sqla.parser import SqlaParser

if TYPE_CHECKING:
    from faststream._internal.endpoint.publisher.proto import PublisherProto
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream._internal.endpoint.subscriber.specification import (
        SubscriberSpecification,
    )
    from faststream.message import StreamMessage
    from faststream.sqla.configs.subscriber import SqlaSubscriberConfig

_CoroutineReturnType = TypeVar("_CoroutineReturnType")


class StopEventSetError(Exception):
    pass


class SqlaSubscriber(TasksMixin, SubscriberUsecase[SqlaInnerMessage]):
    def __init__(
        self,
        config: "SqlaSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[SqlaInnerMessage]",
    ) -> None:
        self.parser = SqlaParser()
        self.config = config
        config.parser = self.parser.parse_message
        config.decoder = self.parser.decode_message
        super().__init__(config, specification, calls)

        self._worker_count = config.max_workers
        self._retry_strategy = config.retry_strategy

        self._max_fetch_interval = config.max_fetch_interval
        self._min_fetch_interval = config.min_fetch_interval
        self._release_stuck_interval = config.release_stuck_interval
        self._flush_interval = config.flush_interval

        self._fetch_batch_size = config.fetch_batch_size
        self._max_not_processed = int(config.fetch_batch_size * config.overfetch_factor)
        self._not_processed_count = 0
        self.graceful_timeout = self._outer_config.graceful_timeout
        self._release_stuck_timeout = config.release_stuck_timeout
        self._max_deliveries = config.max_deliveries

        self._pending_consume_queue: asyncio.Queue[SqlaInnerMessage] = asyncio.Queue()
        self._result_buffer: list[SqlaInnerMessage] = []
        self._last_fetch_was_full = False
        self._stop_event = asyncio.Event()
        self._may_fetch_event = asyncio.Event()
        self._result_buffer_lock = asyncio.Lock()
        self._retry_on_client_error_delay = 5

    @property
    def _client(self) -> SqlaBaseClient:
        return cast("SqlaBaseClient", self.config._outer_config.client)

    @property
    def _queues(self) -> list[str]:
        return [f"{self._outer_config.prefix}{q}" for q in self.config.queues]

    async def start(self) -> None:
        self._stop_event.clear()

        for _ in range(self._worker_count):
            self.add_task(self._worker_loop)

        self._post_start()

        for loop in [self._fetch_loop, self._flush_loop, self._release_stuck_loop]:
            self.add_task(loop)

        self._stop_task = self.add_task(self._stop_event.wait)

        await super().start()

    async def stop(self) -> None:
        self._stop_event.set()
        await self._requeue_pending_consume_queue()
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                self._finalize_workers(), timeout=self.graceful_timeout
            )
        await super().stop()

    @override
    async def should_stop(self) -> None:
        asyncio.create_task(self.stop())  # noqa: RUF006

    async def _finalize_workers(self) -> None:
        """Wait for loops to finish and flush results."""
        await asyncio.gather(*self.tasks, return_exceptions=True)
        task_flush = self.add_task(self._flush_results)
        await task_flush

    def _check_if_may_fetch(self) -> None:
        if self._max_not_processed - self._not_processed_count >= self._fetch_batch_size:
            self._may_fetch_event.set()

    async def _fetch_loop(self) -> None:
        while True:
            if self._stop_event.is_set():
                break
            self._may_fetch_event.clear()

            free_slots = self._max_not_processed - self._not_processed_count
            if free_slots > 0:
                limit = min(self._fetch_batch_size, free_slots)

                try:
                    batch = await self._client.fetch(self._queues, limit=limit)
                except Exception as exc:
                    self._log(logging.ERROR, "SqlaClient error", exc_info=exc)
                    await self._sleep_until_stop_event(self._retry_on_client_error_delay)
                    continue

                for msg in batch:
                    self._not_processed_count += 1
                    await self._pending_consume_queue.put(msg)

                self._check_if_may_fetch()

                self._last_fetch_was_full = len(batch) == limit
                if not self._last_fetch_was_full:
                    await self._sleep_until_stop_event(self._max_fetch_interval)
                    continue

            async with self._local_task(
                asyncio.sleep, func_args=(self._min_fetch_interval,)
            ) as min_fetch_interval_reached_task:
                match await self._wait_for_first_event_or_timeout(
                    self._may_fetch_event,
                    self._stop_event,
                    timeout=self._max_fetch_interval,
                ):
                    case self._may_fetch_event:
                        with suppress(StopEventSetError):
                            await self._wait_until_stop_event(
                                min_fetch_interval_reached_task
                            )
                        continue
                    case self._stop_event | None:
                        continue
                    case _:
                        raise ValueError

    async def _worker_loop(self) -> None:
        while True:
            try:
                message = await self._wait_until_stop_event(
                    self._pending_consume_queue.get()
                )
            except StopEventSetError:
                break

            if message._allow_delivery(
                max_deliveries=self._max_deliveries,
                logger=self._outer_config.logger.logger.logger,
            ):
                message.retry_strategy = self._retry_strategy
                await self.consume(message)
                message._assert_state_updated(self._outer_config.logger.logger.logger)

            self._not_processed_count -= 1
            self._check_if_may_fetch()

            self._buffer_results(message)
            self._pending_consume_queue.task_done()

    async def _flush_loop(self) -> None:
        while True:
            await self._sleep_until_stop_event(self._flush_interval)

            if self._stop_event.is_set():
                break

            try:
                await self._flush_results()
            except Exception as exc:
                self._log(logging.ERROR, "SqlaClient error", exc_info=exc)
                await self._sleep_until_stop_event(self._retry_on_client_error_delay)

    async def _release_stuck_loop(self) -> None:
        while True:
            if self._stop_event.is_set():
                break

            try:
                await self._client.release_stuck(timeout=self._release_stuck_timeout)
            except Exception as exc:
                self._log(logging.ERROR, "SqlaClient error", exc_info=exc)
                await self._sleep_until_stop_event(self._retry_on_client_error_delay)
                continue

            await self._sleep_until_stop_event(self._release_stuck_interval)

    def _buffer_results(
        self, result: SqlaInnerMessage | Iterable[SqlaInnerMessage]
    ) -> None:
        if isinstance(result, Iterable):
            self._result_buffer.extend(result)
        else:
            self._result_buffer.append(result)

    async def _flush_results(self) -> None:
        async with self._result_buffer_lock:  # not technically needed here as of now
            if not (messages := list(self._result_buffer)):
                return
            self._result_buffer.clear()

        to_update = [msg for msg in messages if not msg.to_archive]
        to_archive = [msg for msg in messages if msg.to_archive]

        try:
            await self._client.retry(to_update)
        except Exception:
            self._buffer_results(messages)
            raise

        try:
            await self._client.archive(to_archive)
        except Exception:
            self._buffer_results(to_archive)
            raise

    async def _requeue_pending_consume_queue(self) -> None:
        drained = False

        while True:
            try:
                message = self._pending_consume_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            message._mark_pending()
            self._not_processed_count -= 1
            self._buffer_results(message)
            self._pending_consume_queue.task_done()
            drained = True

        if drained:
            self.add_task(self._flush_results)

    async def _wait_until_stop_event(
        self,
        awaitable: Awaitable[_CoroutineReturnType],
    ) -> _CoroutineReturnType:
        if isinstance(awaitable, asyncio.Task):
            coro_task: asyncio.Task[_CoroutineReturnType] = awaitable
        else:

            async def _runner() -> _CoroutineReturnType:
                return await awaitable

            coro_task = self.add_task(_runner)

        done, _ = await asyncio.wait(
            [coro_task, self._stop_task], return_when=asyncio.FIRST_COMPLETED
        )

        if coro_task in done:
            return await coro_task

        if self._stop_task in done:
            coro_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await coro_task
            raise StopEventSetError

        raise ValueError

    async def _sleep_until_stop_event(self, timeout: float) -> None:
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=timeout)

    async def _wait_for_first_event_or_timeout(
        self,
        *events: asyncio.Event,
        timeout: float | None = None,
    ) -> asyncio.Event | None:
        for event in events:
            if event.is_set():
                return event

        task_to_event: dict[asyncio.Task[bool], asyncio.Event] = {
            self.add_task(event.wait): event for event in events
        }

        try:
            done, _ = await asyncio.wait(
                task_to_event.keys(),
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not done:
                return None

            finished_task = next(iter(done))
            return task_to_event[finished_task]
        finally:
            for task in task_to_event:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*task_to_event.keys(), return_exceptions=True)

    @asynccontextmanager
    async def _local_task(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        func_args: tuple[Any, ...] | None = None,
        func_kwargs: dict[str, Any] | None = None,
    ) -> AsyncGenerator[asyncio.Task[Any], None]:
        task = self.add_task(func, func_args, func_kwargs)
        yield task
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def get_one(
        self,
        *,
        timeout: float = 5,
    ) -> Optional["StreamMessage[SqlaInnerMessage]"]:
        msg = "SqlaBroker doesn't support `get_one`."
        raise FeatureNotSupportedException(msg)

    async def __aiter__(self) -> AsyncIterator["StreamMessage[SqlaInnerMessage]"]:  # type: ignore[override]
        msg = "SqlaBroker doesn't support async iteration."
        raise FeatureNotSupportedException(msg)
        yield  # pragma: no cover

    def _make_response_publisher(
        self,
        message: "StreamMessage[SqlaInnerMessage]",
    ) -> Iterable["PublisherProto"]:
        return ()
