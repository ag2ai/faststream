import asyncio
from contextlib import suppress
from typing import Any, Callable, Coroutine, Sequence, TypeVar

from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.subscriber.specification import SubscriberSpecification
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream._internal.types import MsgType
from faststream.sqla.client import SqlaClient
from faststream.sqla.configs.subscriber import SqlaSubscriberConfig
from faststream.sqla.message import SqlaMessage
from faststream.sqla.parser import SqlaParser


T = TypeVar("T")


class StopEventSetError(Exception):
    pass


class SqlaSubscriber(TasksMixin, SubscriberUsecase[Any]):
    def __init__(
        self,
        config: "SqlaSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[MsgType]",
    ) -> None:
        self.parser = SqlaParser()
        config.parser = self.parser.parse_message
        config.decoder = self.parser.decode_message
        super().__init__(config, specification, calls)

        self._repo = SqlaClient(config.engine)
        self._queue = config.queue
        self._max_fetch_interval = config.max_fetch_interval
        self._min_fetch_interval = config.min_fetch_interval
        self._fetch_batch_size = config.fetch_batch_size
        self._buffer_capacity = int(config.fetch_batch_size * config.overfetch_factor)
        self._flush_interval = config.flush_interval
        self._release_stuck_interval = config.release_stuck_interval
        self._worker_count = config.max_workers
        self._retry_strategy = config.retry_strategy
        self._graceful_shutdown_timeout = config.graceful_shutdown_timeout
        self._release_stuck_timeout = config.release_stuck_timeout

        self._message_queue: asyncio.Queue[SqlaMessage] = asyncio.Queue()
        self._result_buffer: list[SqlaMessage] = []
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        print("start")
        for _ in range(self._worker_count):
            self.add_task(self._worker_loop)
        self._post_start()
        self.add_task(self._fetch_loop)
        self.add_task(self._flush_loop)
        self.add_task(self._release_stuck_loop)
        self._event_task = self.add_task(self._stop_event.wait)
        await super().start()

    async def stop(self) -> None:
        print("stop")
        self._stop_event.set()
        drained = self._drain_acquired()
        if drained:
            for message in drained:
                message.mark_pending()
            self._buffer_results(drained)
            self.add_task(self._flush_results)
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop(), timeout=self._graceful_shutdown_timeout)
        await super().stop()

    async def _stop(self) -> None:
        await asyncio.gather(*self.tasks)
        task_flush = self.add_task(self._flush_results)
        await task_flush

    async def _wait_until_stop_event(self, coro: Callable[[], Coroutine[Any, Any, T]]) -> T:
        coro_task = asyncio.create_task(coro())
        done, _ = await asyncio.wait([coro_task, self._event_task], return_when=asyncio.FIRST_COMPLETED)
        if coro_task in done:
            return await coro_task
        if self._event_task in done:
            raise StopEventSetError

    async def _fetch_loop(self) -> None:
        print("fetch_loop")
        while True:
            if self._stop_event.is_set():
                break

            free_slots = self._buffer_capacity - self._message_queue.qsize()
            if free_slots > 0:
                limit = min(self._fetch_batch_size, free_slots)
                batch = await self._repo.fetch(self._queue, limit=limit)
                for row in batch:
                    await self._message_queue.put(row)

            if free_slots and batch:
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
                message = await self._wait_until_stop_event(self._message_queue.get)
            except StopEventSetError:
                break

            message.retry_strategy = self._retry_strategy
            if message.allow_attempt():
                await self.consume(message)

            self._buffer_results([message])
            self._message_queue.task_done()
        print("worker_loop exit")

    async def _flush_loop(self) -> None:
        print("flush_loop")
        while True:
            if self._stop_event.is_set():
                break
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._flush_interval)
            except asyncio.TimeoutError:
                await self._flush_results()
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
            await self._repo.release_stuck(timeout=self._release_stuck_timeout)
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._release_stuck_interval
                )
            except asyncio.TimeoutError:
                continue
            else:
                break
        print("release_stuck_loop exit")

    def _buffer_results(self, messages: Sequence[SqlaMessage]) -> None:
        print("buffer_results")
        if not messages:
            return
        self._result_buffer.extend(messages)

    async def _flush_results(self) -> None:
        print("flush_results")
        updates = list(self._result_buffer)
        if not updates:
            return
        self._result_buffer.clear()
        await self._repo.retry([item for item in updates if not item.to_archive])
        await self._repo.archive([item for item in updates if item.to_archive])

    def _drain_acquired(self) -> list[SqlaMessage]:
        print("drain_acquired")
        drained: list[SqlaMessage] = []
        while True:
            try:
                message = self._message_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            drained.append(message)
            self._message_queue.task_done()
        return drained
