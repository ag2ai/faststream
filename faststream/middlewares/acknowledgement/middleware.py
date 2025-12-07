import logging
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.middlewares import BaseMiddleware
from faststream.exceptions import (
    AckMessage,
    HandlerException,
    NackMessage,
    RejectMessage,
)

from .config import AckPolicy

if TYPE_CHECKING:
    from types import TracebackType

    from faststream._internal.basic_types import AsyncFuncAny
    from faststream._internal.context.repository import ContextRepo
    from faststream._internal.logger import LoggerState
    from faststream.message import StreamMessage


class AcknowledgementMiddleware:
    def __init__(
        self,
        logger: "LoggerState",
        ack_policy: "AckPolicy",
        extra_options: dict[str, Any],
    ) -> None:
        self.ack_policy = ack_policy
        self.extra_options = extra_options
        self.logger = logger

    def __call__(
        self,
        msg: Any | None,
        context: "ContextRepo",
    ) -> "_AcknowledgementMiddleware":
        return _AcknowledgementMiddleware(
            msg,
            logger=self.logger,
            ack_policy=self.ack_policy,
            extra_options=self.extra_options,
            context=context,
        )


class _AcknowledgementMiddleware(BaseMiddleware):
    def __init__(
        self,
        msg: Any | None,
        /,
        *,
        logger: "LoggerState",
        context: "ContextRepo",
        extra_options: dict[str, Any],
        # can't be created with AckPolicy.MANUAL
        ack_policy: AckPolicy,
    ) -> None:
        super().__init__(msg, context=context)

        self.ack_policy = ack_policy
        self.extra_options = extra_options
        self.logger = logger

        self.message: StreamMessage[Any] | None = None

    async def consume_scope(
        self,
        call_next: "AsyncFuncAny",
        msg: "StreamMessage[Any]",
    ) -> Any:
        self.message = msg
        if self.ack_policy is AckPolicy.ACK_FIRST:
            await self.__ack()

        return await call_next(msg)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> bool | None:
        if self.ack_policy is AckPolicy.ACK_FIRST:
            return False

        if not exc_type:
            await self.__ack()

        elif isinstance(exc_val, HandlerException):
            if isinstance(exc_val, AckMessage):
                await self.__ack(**exc_val.extra_options)

            elif isinstance(exc_val, NackMessage):
                await self.__nack(**exc_val.extra_options)

            elif isinstance(exc_val, RejectMessage):  # pragma: no branch
                await self.__reject(**exc_val.extra_options)

            # Exception was processed and suppressed
            return True

        elif self.ack_policy is AckPolicy.REJECT_ON_ERROR:
            await self.__reject()

        elif self.ack_policy is AckPolicy.NACK_ON_ERROR:
            await self.__nack()

        elif self.ack_policy is AckPolicy.ACK:
            await self.__ack()

        # Exception was not processed
        return False

    async def __ack(self, **exc_extra_options: Any) -> None:
        if self.message:
            try:
                await self.message.ack(**exc_extra_options, **self.extra_options)
            except Exception as er:
                if self.logger is not None:
                    self.logger.log(repr(er), logging.CRITICAL, exc_info=er)

    async def __nack(self, **exc_extra_options: Any) -> None:
        if self.message:
            try:
                await self.message.nack(**exc_extra_options, **self.extra_options)
            except Exception as er:
                if self.logger is not None:
                    self.logger.log(repr(er), logging.CRITICAL, exc_info=er)

    async def __reject(self, **exc_extra_options: Any) -> None:
        if self.message:
            try:
                await self.message.reject(**exc_extra_options, **self.extra_options)
            except Exception as er:
                if self.logger is not None:
                    self.logger.log(repr(er), logging.CRITICAL, exc_info=er)
