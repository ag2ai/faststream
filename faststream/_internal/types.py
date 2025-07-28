from collections.abc import Awaitable, Callable
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    TypeAlias,
    TypeVar,
)

from typing_extensions import (
    ParamSpec,
    TypeVar as TypeVar313,
)

from faststream._internal.basic_types import AsyncFuncAny
from faststream._internal.context.repository import ContextRepo
from faststream.message import StreamMessage
from faststream.response.response import PublishCommand

if TYPE_CHECKING:
    from faststream._internal.middlewares import BaseMiddleware


AnyMsg = TypeVar313("AnyMsg", default=Any)
AnyMsg_contra = TypeVar313("AnyMsg_contra", default=Any, contravariant=True)
MsgType = TypeVar("MsgType")
StreamMsg = TypeVar("StreamMsg", bound=StreamMessage[Any])
ConnectionType = TypeVar("ConnectionType")
PublishCommandType = TypeVar313(
    "PublishCommandType",
    bound=PublishCommand,
    default=PublishCommand,
)
PublishCommandType_contra = TypeVar313(
    "PublishCommandType_contra",
    bound=PublishCommand,
    default=PublishCommand,
    contravariant=True,
)

SyncFilter: TypeAlias = Callable[[StreamMsg], bool]
AsyncFilter: TypeAlias = Callable[[StreamMsg], Awaitable[bool]]
Filter: TypeAlias = SyncFilter[StreamMsg] | AsyncFilter[StreamMsg]

SyncCallable: TypeAlias = Callable[
    [Any],
    Any,
]
AsyncCallable: TypeAlias = AsyncFuncAny
AsyncCustomCallable: TypeAlias = (
    AsyncFuncAny | Callable[[Any, AsyncFuncAny], Awaitable[Any]]
)
CustomCallable: TypeAlias = AsyncCustomCallable | SyncCallable

P_HandlerParams = ParamSpec("P_HandlerParams")
T_HandlerReturn = TypeVar("T_HandlerReturn")


AsyncWrappedHandlerCall: TypeAlias = Callable[
    [StreamMessage[MsgType]],
    Awaitable[T_HandlerReturn | None],
]
SyncWrappedHandlerCall: TypeAlias = Callable[
    [StreamMessage[MsgType]],
    T_HandlerReturn | None,
]
WrappedHandlerCall: TypeAlias = (
    AsyncWrappedHandlerCall[MsgType, T_HandlerReturn]
    | SyncWrappedHandlerCall[MsgType, T_HandlerReturn]
)


class BrokerMiddleware(Protocol[AnyMsg_contra, PublishCommandType]):
    """Middleware builder interface."""

    def __call__(
        self,
        msg: AnyMsg_contra | None,
        /,
        *,
        context: ContextRepo,
    ) -> "BaseMiddleware[PublishCommandType]": ...


SubscriberMiddleware: TypeAlias = Callable[
    [AsyncFuncAny, MsgType],
    MsgType,
]


class PublisherMiddleware(Protocol[PublishCommandType]):
    """Publisher middleware interface."""

    def __call__(
        self,
        call_next: Callable[[PublishCommandType], Awaitable[Any]],
        cmd: PublishCommandType,
    ) -> Any: ...
