import asyncio
from contextlib import suppress
import contextlib
import logging
from typing import Any, Callable, Coroutine, Iterable, Sequence, TypeVar

from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.subscriber.specification import SubscriberSpecification
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream._internal.types import MsgType
from faststream.sqla.client import SqlaPostgresClient, create_sqla_client
from faststream.sqla.configs.subscriber import SqlaSubscriberConfig
from faststream.sqla.message import SqlaInnerMessage
from faststream.sqla.parser import SqlaParser


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
        config.parser = self.parser.parse_message
        config.decoder = self.parser.decode_message
        super().__init__(config, specification, calls)

        self._repo = create_sqla_client(config.engine)
        self._queues = config.queues
        self._worker_count = config.max_workers
        self._retry_strategy = config.retry_strategy
        
        self._max_fetch_interval = config.max_fetch_interval
        self._min_fetch_interval = config.min_fetch_interval
        self._release_stuck_interval = config.release_stuck_interval
        self._flush_interval = config.flush_interval
        
        self._fetch_batch_size = config.fetch_batch_size
        self._awaiting_consume_queue_capacity = int(config.fetch_batch_size * config.overfetch_factor)
        self.graceful_timeout = self._outer_config.graceful_timeout
        self._release_stuck_timeout = config.release_stuck_timeout
        self._max_deliveries = config.max_deliveries

        self._awaiting_consume_queue: asyncio.Queue[SqlaInnerMessage] = asyncio.Queue()
        self._result_buffer: list[SqlaInnerMessage] = []
        self._stop_event = asyncio.Event()
        self._result_buffer_lock = asyncio.Lock()
        self._retry_on_client_error_delay = 5

    async def start(self) -> None:
        print("start")
        self._stop_event.clear()
        for _ in range(self._worker_count):
            self.add_task(self._worker_loop)
        self._post_start()
        self.add_task(self._fetch_loop)
        self.add_task(self._flush_loop)
        self.add_task(self._release_stuck_loop)
        self._stop_task = self.add_task(self._stop_event.wait)
        await super().start()

    async def stop(self) -> None:
        print("stop")
        self._stop_event.set()
        await self._requeue_awaiting_consume_queue()
        print("with suppress(asyncio.TimeoutError):")
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop(), timeout=self.graceful_timeout)
        print("await super().stop()")
        await super().stop()

    async def _stop(self) -> None:
        print("await asyncio.gather(*self.tasks)")
        await asyncio.gather(*self.tasks)
        task_flush = self.add_task(self._flush_results)
        await task_flush

    async def _wait_until_stop_event(self, coro: Coroutine[Any, Any, _CoroutineReturnType]) -> _CoroutineReturnType:
        coro_task = asyncio.create_task(coro)
        done, _ = await asyncio.wait([coro_task, self._stop_task], return_when=asyncio.FIRST_COMPLETED)
        
        if coro_task in done:
            return await coro_task
        
        if self._stop_task in done:
            coro_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await coro_task
            raise StopEventSetError

    async def _fetch_loop(self) -> None:
        print("fetch_loop")
        while True:
            if self._stop_event.is_set():
                break

            free_slots = self._awaiting_consume_queue_capacity - self._awaiting_consume_queue.qsize()
            if free_slots > 0:
                limit = min(self._fetch_batch_size, free_slots)
                
                try:
                    batch = await self._repo.fetch(self._queues, limit=limit)
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    self._log(logging.ERROR, "SqlaClient error", exc_info=exc)
                    await asyncio.sleep(self._retry_on_client_error_delay)
                    continue
                
                for msg in batch:
                    await self._awaiting_consume_queue.put(msg)

            if free_slots and len(batch) == limit:
                timeout_ = self._min_fetch_interval
            else:
                timeout_ = self._max_fetch_interval

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=timeout_)
            except asyncio.TimeoutError:
                continue
            else:
                break
        print("fetch_loop exit")

    async def _worker_loop(self) -> None:
        print("worker_loop")
        while True:
            try:
                message = await self._wait_until_stop_event(self._awaiting_consume_queue.get())
            except StopEventSetError:
                break

            if message._allow_delivery(
                max_deliveries=self._max_deliveries,
                logger=self._outer_config.logger.logger.logger
            ):
                message.retry_strategy = self._retry_strategy
                await self.consume(message)
                message._assert_state_updated(self._outer_config.logger.logger.logger)

            self._buffer_results(message)
            self._awaiting_consume_queue.task_done()
        print("worker_loop exit")

    async def _flush_loop(self) -> None:
        print("flush_loop")
        while True:
            if self._stop_event.is_set():
                break

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._flush_interval)
            except asyncio.TimeoutError:
                try:
                    await self._flush_results()
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    self._log(logging.ERROR, "SqlaClient error", exc_info=exc)
                    await asyncio.sleep(self._retry_on_client_error_delay)
                    continue
                
                continue
            else:
                break
        await self._flush_results()
        print("flush_loop exit")

    async def _release_stuck_loop(self) -> None:
        print("release_stuck_loop")
        while True:
            if self._stop_event.is_set():
                break
            
            try:
                await self._repo.release_stuck(timeout=self._release_stuck_timeout)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log(logging.ERROR, "SqlaClient error", exc_info=exc)
                await asyncio.sleep(self._retry_on_client_error_delay)
                continue

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._release_stuck_interval
                )
            except asyncio.TimeoutError:
                continue
            else:
                break
        print("release_stuck_loop exit")

    def _buffer_results(self, result: SqlaInnerMessage | Iterable[SqlaInnerMessage]) -> None:
        print("buffer_results")
        if isinstance(result, Iterable):
            self._result_buffer.extend(result)
        else:
            self._result_buffer.append(result)

    async def _flush_results(self) -> None:
        print("flush_results")
        async with self._result_buffer_lock: # not technically needed here as of now
            if not (messages := list(self._result_buffer)):
                return
            self._result_buffer.clear()

        to_update = [msg for msg in messages if not msg.to_archive]
        to_archive = [msg for msg in messages if msg.to_archive]

        try:
            await self._repo.retry(to_update)
        except Exception:
            self._buffer_results(to_update)
            raise
        
        try:
            await self._repo.archive(to_archive)
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
