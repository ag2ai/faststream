import asyncio
import logging
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar

from typing_extensions import ParamSpec

from faststream._internal.middlewares import BaseMiddleware
from faststream._internal.types import MiddlewareMsgType
from faststream.exceptions import (
    AckMessage,
    HandlerException,
    NackMessage,
    RejectMessage,
)
from faststream.response.response import PublishCommand

from .config import AckPolicy

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from faststream._internal.basic_types import AsyncFuncAny
    from faststream._internal.context.repository import ContextRepo
    from faststream._internal.logger import LoggerState
    from faststream._internal.types import Parser
    from faststream.message import StreamMessage


_CallParams = ParamSpec("_CallParams")
_ReturnT = TypeVar("_ReturnT")


class AcknowledgementMiddleware(Generic[MiddlewareMsgType]):
    def __init__(
        self,
        logger: "LoggerState",
        ack_policy: "AckPolicy",
        fallback_parser: "Parser[MiddlewareMsgType, StreamMessage[Any]]",
    ) -> None:
        self.ack_policy = ack_policy
        self.logger = logger
        self.fallback_parser = fallback_parser

    def __call__(
        self,
        msg: MiddlewareMsgType | None,
        context: "ContextRepo",
    ) -> "_AcknowledgementMiddleware[MiddlewareMsgType]":
        return _AcknowledgementMiddleware(
            msg,
            logger=self.logger,
            ack_policy=self.ack_policy,
            context=context,
            fallback_parser=self.fallback_parser,
        )


class _AcknowledgementMiddleware(
    BaseMiddleware[PublishCommand, MiddlewareMsgType | None],
):
    __slots__ = ("ack_policy", "fallback_parser", "logger", "message")

    def __init__(
        self,
        msg: MiddlewareMsgType | None,
        /,
        *,
        logger: "LoggerState",
        context: "ContextRepo",
        # can't be created with AckPolicy.MANUAL
        ack_policy: AckPolicy,
        fallback_parser: "Parser[MiddlewareMsgType, StreamMessage[Any]]",
    ) -> None:
        super().__init__(msg, context=context)

        self.ack_policy = ack_policy
        self.logger = logger
        self.fallback_parser = fallback_parser

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
            if self.message is None:
                await self.__ack()
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

        elif isinstance(exc_val, asyncio.CancelledError):
            return False

        elif self.ack_policy is AckPolicy.REJECT_ON_ERROR:
            await self.__reject()

        elif self.ack_policy is AckPolicy.NACK_ON_ERROR:
            await self.__nack()

        elif self.ack_policy is AckPolicy.ACK:
            await self.__ack()

        # Exception was not processed
        return False

    async def __do_with_logging(
        self,
        func: "Callable[_CallParams, Awaitable[_ReturnT]]",
        /,
        *args: _CallParams.args,
        **kwargs: _CallParams.kwargs,
    ) -> _ReturnT | None:
        try:
            return await func(*args, **kwargs)
        except Exception as er:
            if self.logger is not None:
                self.logger.log(repr(er), logging.CRITICAL, exc_info=er)
        return None

    async def __resolve_message(self) -> "StreamMessage[Any] | None":
        if self.message is not None:
            return self.message
        if self.msg is None:
            return None
        return await self.__do_with_logging(self.fallback_parser, self.msg)

    async def __ack(self, **exc_extra_options: Any) -> None:
        if message := await self.__resolve_message():
            await self.__do_with_logging(message.ack, **exc_extra_options)

    async def __nack(self, **exc_extra_options: Any) -> None:
        if message := await self.__resolve_message():
            await self.__do_with_logging(message.nack, **exc_extra_options)

    async def __reject(self, **exc_extra_options: Any) -> None:
        if message := await self.__resolve_message():
            await self.__do_with_logging(message.reject, **exc_extra_options)
