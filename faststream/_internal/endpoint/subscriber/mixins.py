import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any, Protocol

import anyio
from typing_extensions import override

from faststream._internal.types import AnyMsg_contra, MsgType

from .supervisor import TaskCallbackSupervisor

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

    from faststream._internal.configs import BrokerConfig


class TasksSupervisor:
    __slots__ = ("broker_config", "tasks")

    def __init__(self, broker_config: "BrokerConfig") -> None:
        self.tasks: list[asyncio.Task[Any]] = []
        self.broker_config = broker_config

    def add_task(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        func_args: tuple[Any, ...] | None = None,
        func_kwargs: dict[str, Any] | None = None,
        *,
        restart_on_failure: bool = True,
    ) -> None:
        args = func_args or ()
        kwargs = func_kwargs or {}
        task = asyncio.create_task(func(*args, **kwargs))
        callback = TaskCallbackSupervisor(
            func,
            func_args,
            func_kwargs,
            self,
            self.broker_config,
            restart_on_failure=restart_on_failure,
        )
        task.add_done_callback(callback)
        self.tasks.append(task)

    async def stop(self) -> None:
        """Clean up handler subscription, cancel consume task in graceful mode."""
        for task in self.tasks:
            if not task.done():
                task.cancel()

        self.tasks.clear()


class ConsumeDispatcherProto(Protocol[AnyMsg_contra]):
    __slots__ = ()

    async def dispatch(self, msg: AnyMsg_contra) -> Any: ...

    async def start(self) -> None:  # ruff: ignore[no-self-use]
        return None

    async def stop(self) -> None:  # ruff: ignore[no-self-use]
        return None


class DirectDispatcher(ConsumeDispatcherProto[MsgType]):
    __slots__ = ("consume_func",)

    def __init__(self, consume_func: Callable[[MsgType], Awaitable[Any]]) -> None:
        self.consume_func = consume_func

    @override
    async def dispatch(self, msg: MsgType) -> Any:
        return await self.consume_func(msg)


class ConcurrentDispatcher(ConsumeDispatcherProto[MsgType]):
    __slots__ = (
        "consume_func",
        "limiter",
        "max_workers",
        "receive_stream",
        "send_stream",
        "tasks_supervisor",
    )

    send_stream: "MemoryObjectSendStream[MsgType]"
    receive_stream: "MemoryObjectReceiveStream[MsgType]"

    def __init__(
        self,
        tasks_supervisor: TasksSupervisor,
        max_workers: int,
        consume_func: Callable[[MsgType], Awaitable[Any]],
    ) -> None:
        self.max_workers = max_workers
        self.consume_func = consume_func
        self.limiter = anyio.Semaphore(max_workers)
        # Closed until `start`, so a subscriber that is only declared holds nothing open
        self.send_stream, self.receive_stream = _closed_queue()

        self.tasks_supervisor = tasks_supervisor

    @override
    async def start(self) -> None:
        self.send_stream, self.receive_stream = anyio.create_memory_object_stream(
            max_buffer_size=self.max_workers,
        )
        self.tasks_supervisor.add_task(self._serve_consume_queue)

    @override
    async def stop(self) -> None:
        self.send_stream.close()
        self.receive_stream.close()

    async def _serve_consume_queue(
        self,
    ) -> None:
        """Endless task consuming messages from in-memory queue.

        Suitable to batch messages by amount, timestamps, etc and call `consume` for this batches.
        """
        async with anyio.create_task_group() as tg:
            async for msg in self.receive_stream:
                _ = tg.start_soon(self._consume_msg, msg)

    async def _consume_msg(self, msg: "MsgType") -> None:
        """Proxy method to call `self.consume` with semaphore block."""
        async with self.limiter:
            await self.consume_func(msg)

    @override
    async def dispatch(self, msg: "MsgType") -> None:
        async with self.limiter:
            await self.send_stream.send(msg)


def _closed_queue() -> tuple[
    "MemoryObjectSendStream[Any]", "MemoryObjectReceiveStream[Any]"
]:
    send_stream, receive_stream = anyio.create_memory_object_stream[Any]()
    send_stream.close()
    receive_stream.close()
    return send_stream, receive_stream
