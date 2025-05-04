import asyncio
import inspect
from collections.abc import Awaitable, Iterable
from contextlib import AsyncExitStack, suppress
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Union,
    cast,
)

import anyio
from typing_extensions import Self

from faststream._internal.types import MsgType
from faststream._internal.utils.functions import return_input, to_async
from faststream.message.source_type import SourceType

if TYPE_CHECKING:
    from types import TracebackType

    from faststream._internal.types import (
        AsyncCallable,
        CustomCallable,
        SyncCallable,
    )
    from faststream.message import StreamMessage
    from faststream.middlewares import BaseMiddleware


async def process_msg(
    msg: Optional[MsgType],
    middlewares: Iterable["BaseMiddleware"],
    parser: Callable[[MsgType], Awaitable["StreamMessage[MsgType]"]],
    decoder: Callable[["StreamMessage[MsgType]"], "Any"],
    source_type: SourceType = SourceType.CONSUME,
) -> Optional["StreamMessage[MsgType]"]:
    if msg is None:
        return None

    async with AsyncExitStack() as stack:
        return_msg: Callable[
            [StreamMessage[MsgType]],
            Awaitable[StreamMessage[MsgType]],
        ] = return_input

        for m in middlewares:
            await stack.enter_async_context(m)
            return_msg = partial(m.consume_scope, return_msg)

        parsed_msg = await parser(msg)
        parsed_msg._source_type = source_type
        parsed_msg.set_decoder(decoder)
        return await return_msg(parsed_msg)

    error_msg = "unreachable"
    raise AssertionError(error_msg)


async def default_filter(msg: "StreamMessage[Any]") -> bool:
    """A function to filter stream messages."""
    return not msg.processed


class MultiLock:
    """A class representing a multi lock.

    This lock can be acquired multiple times.
    `wait_release` method waits for all locks will be released.
    """

    def __init__(self) -> None:
        """Initialize a new instance of the class."""
        self.queue: asyncio.Queue[None] = asyncio.Queue()

    def __enter__(self) -> Self:
        """Enter the context."""
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional["TracebackType"],
    ) -> None:
        """Exit the context."""
        self.release()

    def acquire(self) -> None:
        """Acquire lock."""
        self.queue.put_nowait(None)

    def release(self) -> None:
        """Release lock."""
        with suppress(asyncio.QueueEmpty, ValueError):
            self.queue.get_nowait()
            self.queue.task_done()

    @property
    def qsize(self) -> int:
        """Return the size of the queue."""
        return self.queue.qsize()

    @property
    def empty(self) -> bool:
        """Return whether the queue is empty."""
        return self.queue.empty()

    async def wait_release(self, timeout: Optional[float] = None) -> None:
        """Wait for the queue to be released.

        Using for graceful shutdown.
        """
        if timeout:
            with anyio.move_on_after(timeout):
                await self.queue.join()


def resolve_custom_func(
    custom_func: Optional["CustomCallable"],
    default_func: "AsyncCallable",
) -> "AsyncCallable":
    """Resolve a custom parser/decoder with default one."""
    if custom_func is None:
        return default_func

    original_params = inspect.signature(custom_func).parameters

    if len(original_params) == 1:
        return to_async(cast("Union[SyncCallable, AsyncCallable]", custom_func))

    name = tuple(original_params.items())[1][0]
    return partial(to_async(custom_func), **{name: default_func})
