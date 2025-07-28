import asyncio
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Dict,
    Generic,
    Optional,
    Set,
    Tuple,
    Union,
)

import anyio

from faststream.broker.types import MsgType

from ._supervisor import TaskCallbackSupervisor
from .usecase import SubscriberUsecase

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream


class TasksMixin(SubscriberUsecase[Any]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.tasks: Set[asyncio.Task[Any]] = set()

    def add_task(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        func_args: Optional[Tuple[Any]] = None,
        func_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        args: Union[Tuple[Any], Tuple[()]] = func_args or ()
        kwargs: Dict[str, Any] = func_kwargs or {}
        task = asyncio.create_task(func(*args, **kwargs))
        callback = TaskCallbackSupervisor(func, func_args, func_kwargs, self)
        task.add_done_callback(callback)
        self.tasks.add(task)

    async def stop(self) -> None:
        """Clean up handler subscription, cancel consume task in graceful mode."""
        await super().stop()

        for task in self.tasks:
            if not task.done():
                task.cancel()

        self.tasks = set()


class ConcurrentMixin(TasksMixin, Generic[MsgType]):
    send_stream: "MemoryObjectSendStream[MsgType]"
    receive_stream: "MemoryObjectReceiveStream[MsgType]"

    def __init__(
        self,
        *args: Any,
        max_workers: int,
        **kwargs: Any,
    ) -> None:
        self.max_workers = max_workers

        self.send_stream, self.receive_stream = anyio.create_memory_object_stream(
            max_buffer_size=max_workers
        )
        self.limiter = anyio.Semaphore(max_workers)

        super().__init__(*args, **kwargs)

    def start_consume_task(self) -> None:
        self.add_task(self._serve_consume_queue)

    async def _serve_consume_queue(
        self,
    ) -> None:
        """Endless task consuming messages from in-memory queue.

        Suitable to batch messages by amount, timestamps, etc and call `consume` for this batches.
        """
        async with anyio.create_task_group() as tg:
            async for msg in self.receive_stream:
                tg.start_soon(self._consume_msg, msg)

    async def _consume_msg(
        self,
        msg: "MsgType",
    ) -> None:
        """Proxy method to call `self.consume` with semaphore block."""
        async with self.limiter:
            await self.consume(msg)

    async def _put_msg(self, msg: "MsgType") -> None:
        """Proxy method to put msg into in-memory queue with semaphore block."""
        async with self.limiter:
            await self.send_stream.send(msg)
