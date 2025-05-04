from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.middlewares import BaseMiddleware
from faststream.exceptions import IgnoredException
from faststream.message.source_type import SourceType

if TYPE_CHECKING:
    from types import TracebackType

    from faststream._internal.basic_types import AsyncFuncAny
    from faststream._internal.context.repository import ContextRepo
    from faststream._internal.state.logger import LoggerState
    from faststream.message import StreamMessage


class CriticalLogMiddleware:
    def __init__(self, logger: "LoggerState") -> None:
        """Initialize the class."""
        self.logger = logger

    def __call__(
        self,
        msg: Optional[Any],
        /,
        *,
        context: "ContextRepo",
    ) -> "_LoggingMiddleware":
        return _LoggingMiddleware(
            logger=self.logger,
            msg=msg,
            context=context,
        )


class _LoggingMiddleware(BaseMiddleware):
    """A middleware class for logging critical errors."""

    def __init__(
        self,
        *,
        logger: "LoggerState",
        context: "ContextRepo",
        msg: Optional[Any],
    ) -> None:
        super().__init__(msg, context=context)
        self.logger = logger
        self._source_type = SourceType.CONSUME

    async def consume_scope(
        self,
        call_next: "AsyncFuncAny",
        msg: "StreamMessage[Any]",
    ) -> Any:
        source_type = self._source_type = msg._source_type

        if source_type is not SourceType.RESPONSE:
            self.logger.log(
                "Received",
                extra=self.context.get_local("log_context", {}),
            )

        return await call_next(msg)

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> bool:
        """Asynchronously called after processing."""
        if self._source_type is not SourceType.RESPONSE:
            c = self.context.get_local("log_context", {})

            if exc_type:
                # TODO: move critical logging to `subscriber.consume()` method
                if issubclass(exc_type, IgnoredException):
                    self.logger.log(
                        message=str(exc_val),
                        extra=c,
                    )

                else:
                    self.logger.log(
                        message=f"{exc_type.__name__}: {exc_val}",
                        exc_info=exc_val,
                        extra=c,
                    )

            self.logger.log(message="Processed", extra=c)

        await super().__aexit__(exc_type, exc_val, exc_tb)

        # Exception was not processed
        return False
