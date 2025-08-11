import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional

import anyio
from typing_extensions import Self

if TYPE_CHECKING:
    from types import TracebackType

    from faststream.message import StreamMessage


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
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
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

    async def wait_release(self, timeout: float | None = None) -> None:
        """Wait for the queue to be released.

        Using for graceful shutdown.
        """
        if timeout:
            with anyio.move_on_after(timeout):
                await self.queue.join()
