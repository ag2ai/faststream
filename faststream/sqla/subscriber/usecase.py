import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Coroutine, Iterable
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional, TypeVar

from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream.exceptions import FeatureNotSupportedException
from faststream.sqla.client import SqlaBaseClient
from faststream.sqla.message import SqlaInnerMessage
from faststream.sqla.parser import SqlaParser

if TYPE_CHECKING:
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream._internal.endpoint.subscriber.specification import (
        SubscriberSpecification,
    )
    from faststream._internal.endpoint.publisher.proto import PublisherProto
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
        self._awaiting_consume_queue_capacity = int(
            config.fetch_batch_size * config.overfetch_factor
        )
        self.graceful_timeout = self._outer_config.graceful_timeout
        self._release_stuck_timeout = config.release_stuck_timeout
        self._max_deliveries = config.max_deliveries

        self._awaiting_consume_queue: asyncio.Queue[SqlaInnerMessage] = asyncio.Queue()
        self._result_buffer: list[SqlaInnerMessage] = []
        self._stop_event = asyncio.Event()
        self._result_buffer_lock = asyncio.Lock()
        self._retry_on_client_error_delay = 5

    async def get_one(
        self, *, timeout: float = 5,
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

    @property
    def _client(self) -> SqlaBaseClient:
        return self.config._outer_config.client

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
        await self._requeue_awaiting_consume_queue()
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop(), timeout=self.graceful_timeout)
        await super().stop()

    async def _stop(self) -> None:
        await asyncio.gather(*self.tasks)
        task_flush = self.add_task(self._flush_results)
        await task_flush

    async def _wait_until_stop_event(
        self, coro: Coroutine[Any, Any, _CoroutineReturnType]
    ) -> _CoroutineReturnType:
        coro_task = asyncio.create_task(coro)
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

    async def _fetch_loop(self) -> None:
        while True:
            if self._stop_event.is_set():
                break

            free_slots = (
                self._awaiting_consume_queue_capacity
                - self._awaiting_consume_queue.qsize()
            )
            if free_slots > 0:
                limit = min(self._fetch_batch_size, free_slots)

                try:
                    batch = await self._client.fetch(self._queues, limit=limit)
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    self._log(logging.ERROR, "SqlaClient error", exc_info=exc)
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self._stop_event.wait(), timeout=self._retry_on_client_error_delay)
                    continue

                for msg in batch:
                    await self._awaiting_consume_queue.put(msg)

            if free_slots and len(batch) == limit:
                timeout_ = self._min_fetch_interval
            else:
                timeout_ = self._max_fetch_interval

            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=timeout_)

    async def _worker_loop(self) -> None:
        while True:
            try:
                message = await self._wait_until_stop_event(
                    self._awaiting_consume_queue.get()
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

            self._buffer_results(message)
            self._awaiting_consume_queue.task_done()

    async def _flush_loop(self) -> None:
        while True:
            if self._stop_event.is_set():
                break

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._flush_interval
                )
            except asyncio.TimeoutError:
                try:
                    await self._flush_results()
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    self._log(logging.ERROR, "SqlaClient error", exc_info=exc)
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self._stop_event.wait(), timeout=self._retry_on_client_error_delay)

        await self._flush_results()

    async def _release_stuck_loop(self) -> None:
        while True:
            if self._stop_event.is_set():
                break

            try:
                await self._client.release_stuck(timeout=self._release_stuck_timeout)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(logging.ERROR, "SqlaClient error", exc_info=exc)
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._retry_on_client_error_delay)
                continue

            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._release_stuck_interval
                )

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
            self._buffer_results(to_update)
            raise

        try:
            await self._client.archive(to_archive)
        except Exception:
            self._buffer_results(to_archive)
            raise

    async def _requeue_awaiting_consume_queue(self) -> None:
        drained = False

        while True:
            try:
                message = self._awaiting_consume_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            message._mark_pending()
            self._buffer_results(message)
            self._awaiting_consume_queue.task_done()
            drained = True

        if drained:
            self.add_task(self._flush_results)
