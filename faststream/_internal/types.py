from collections.abc import Awaitable
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Protocol,
    TypeVar,
    Union,
)

from typing_extensions import (
    ParamSpec,
    TypeAlias,
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
Msg_contra = TypeVar("Msg_contra", contravariant=True)
StreamMsg = TypeVar("StreamMsg", bound=StreamMessage[Any])
ConnectionType = TypeVar("ConnectionType")
PublishCommandType = TypeVar313(
    "PublishCommandType",
    bound=PublishCommand,
    default=Any,
)

SyncFilter: TypeAlias = Callable[[StreamMsg], bool]
AsyncFilter: TypeAlias = Callable[[StreamMsg], Awaitable[bool]]
Filter: TypeAlias = Union[
    SyncFilter[StreamMsg],
    AsyncFilter[StreamMsg],
]

SyncCallable: TypeAlias = Callable[
    [Any],
    Any,
]
AsyncCallable: TypeAlias = AsyncFuncAny
AsyncCustomCallable: TypeAlias = Union[
    AsyncFuncAny,
    Callable[
        [Any, AsyncFuncAny],
        Awaitable[Any],
    ],
]
CustomCallable: TypeAlias = Union[
    AsyncCustomCallable,
    SyncCallable,
]

P_HandlerParams = ParamSpec("P_HandlerParams")
T_HandlerReturn = TypeVar("T_HandlerReturn")


AsyncWrappedHandlerCall: TypeAlias = Callable[
    [StreamMessage[MsgType]],
    Awaitable[Optional[T_HandlerReturn]],
]
SyncWrappedHandlerCall: TypeAlias = Callable[
    [StreamMessage[MsgType]],
    Optional[T_HandlerReturn],
]
WrappedHandlerCall: TypeAlias = Union[
    AsyncWrappedHandlerCall[MsgType, T_HandlerReturn],
    SyncWrappedHandlerCall[MsgType, T_HandlerReturn],
]


class BrokerMiddleware(Protocol[AnyMsg_contra, PublishCommandType]):
    """Middleware builder interface."""

    def __call__(
        self,
        msg: Optional[AnyMsg_contra],
        /,
        *,
        context: ContextRepo,
    ) -> "BaseMiddleware[PublishCommandType]": ...


SubscriberMiddleware: TypeAlias = Callable[
    [AsyncFuncAny, MsgType],
    MsgType,
]


class PublisherMiddleware(Protocol):
    """Publisher middleware interface."""

    def __call__(
        self,
        call_next: Callable[[PublishCommand], Awaitable[Any]],
        cmd: PublishCommand,
    ) -> Any: ...
