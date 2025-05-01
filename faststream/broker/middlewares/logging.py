import logging
from typing import TYPE_CHECKING, Any, Optional, Type

from typing_extensions import Self

from faststream.broker.middlewares.base import BaseMiddleware
from faststream.exceptions import IgnoredException
from faststream.utils.context.repository import context

if TYPE_CHECKING:
    from types import TracebackType

    from faststream.broker.message import StreamMessage
    from faststream.types import LoggerProto


class CriticalLogMiddleware(BaseMiddleware):
    """A middleware class for logging critical errors."""

    def __init__(
        self,
        logger: Optional["LoggerProto"],
        log_level: int,
    ) -> None:
        """Initialize the class."""
        self.logger = logger
        self.log_level = log_level

    def __call__(self, msg: Optional[Any]) -> Self:
        """Call the object with a message."""
        self.msg = msg
        return self

    async def on_consume(
        self,
        msg: "StreamMessage[Any]",
    ) -> "StreamMessage[Any]":
        if self.logger is not None:
            c = context.get_local("log_context", {})
            self.logger.log(self.log_level, "Received", extra=c)

        return await super().on_consume(msg)

    async def after_processed(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> bool:
        """Asynchronously called after processing."""
        if self.logger is not None:
            c = context.get_local("log_context", {})

            if exc_type:
                if issubclass(exc_type, IgnoredException):
                    self.logger.log(
                        self.log_level,
                        exc_val,
                        extra=c,
                    )
                else:
                    self.logger.log(
                        logging.ERROR,
                        f"{exc_type.__name__}: {exc_val}",
                        exc_info=exc_val,
                        extra=c,
                    )

            self.logger.log(self.log_level, "Processed", extra=c)

        await super().after_processed(exc_type, exc_val, exc_tb)

        # Exception was not processed
        return False
