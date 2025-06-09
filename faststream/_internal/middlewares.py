from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Generic, Optional

from typing_extensions import Self

from faststream._internal.types import AnyMsg, PublishCommandType

if TYPE_CHECKING:
    from types import TracebackType

    from faststream._internal.basic_types import AsyncFuncAny
    from faststream._internal.context.repository import ContextRepo
    from faststream.message import StreamMessage


class BaseMiddleware(Generic[PublishCommandType, AnyMsg]):
    """A base middleware class."""

    def __init__(
        self,
        msg: AnyMsg | None,
        /,
        *,
        context: "ContextRepo",
    ) -> None:
        self.msg = msg
        self.context = context

    async def on_receive(self) -> None:
        """Hook to call on message receive."""

    async def after_processed(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> bool | None:
        """Asynchronously called after processing."""
        return False

    async def __aenter__(self) -> Self:
        await self.on_receive()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> bool | None:
        """Exit the asynchronous context manager."""
        return await self.after_processed(exc_type, exc_val, exc_tb)

    async def on_consume(
        self,
        msg: "StreamMessage[AnyMsg]",
    ) -> "StreamMessage[AnyMsg]":
        """This option was deprecated and will be removed in 0.7.0. Please, use `consume_scope` instead."""
        return msg

    async def after_consume(self, err: Exception | None) -> None:
        """This option was deprecated and will be removed in 0.7.0. Please, use `consume_scope` instead."""
        if err is not None:
            raise err

    async def consume_scope(
        self,
        call_next: "AsyncFuncAny",
        msg: "StreamMessage[AnyMsg]",
    ) -> Any:
        """Asynchronously consumes a message and returns an asynchronous iterator of decoded messages."""
        err: Exception | None = None
        try:
            result = await call_next(await self.on_consume(msg))

        except Exception as e:
            err = e

        else:
            return result

        finally:
            await self.after_consume(err)

    async def on_publish(
        self,
        msg: PublishCommandType,
    ) -> PublishCommandType:
        """This option was deprecated and will be removed in 0.7.0. Please, use `publish_scope` instead."""
        return msg

    async def after_publish(
        self,
        err: Exception | None,
    ) -> None:
        """This option was deprecated and will be removed in 0.7.0. Please, use `publish_scope` instead."""
        if err is not None:
            raise err

    async def publish_scope(
        self,
        call_next: Callable[[PublishCommandType], Awaitable[Any]],
        cmd: PublishCommandType,
    ) -> Any:
        """Publish a message and return an async iterator."""
        err: Exception | None = None
        try:
            result = await call_next(await self.on_publish(cmd))

        except Exception as e:
            err = e

        else:
            return result

        finally:
            await self.after_publish(err)
