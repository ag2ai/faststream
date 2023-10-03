from typing import Any, Awaitable, Callable, Optional, Protocol, Tuple, TypeVar, Union

from faststream._compat import ParamSpec
from faststream.broker.message import StreamMessage
from faststream.types import DecodedMessage, SendableMessage

Decoded = TypeVar("Decoded", bound=DecodedMessage)
MsgType = TypeVar("MsgType")
StreamMsg = TypeVar("StreamMsg", bound=StreamMessage[Any])
ConnectionType = TypeVar("ConnectionType")

SyncFilter = Callable[[StreamMsg], bool]
AsyncFilter = Callable[[StreamMsg], Awaitable[bool]]
Filter = Union[
    SyncFilter[StreamMsg],
    AsyncFilter[StreamMsg],
]

SyncParser = Callable[
    [MsgType],
    StreamMsg,
]
AsyncParser = Callable[
    [MsgType],
    Awaitable[StreamMsg],
]
AsyncCustomParser = Union[
    AsyncParser[MsgType, StreamMsg],
    Callable[
        [MsgType, AsyncParser[MsgType, StreamMsg]],
        Awaitable[StreamMsg],
    ],
]
Parser = Union[
    AsyncParser[MsgType, StreamMsg],
    SyncParser[MsgType, StreamMsg],
]
CustomParser = Union[
    AsyncCustomParser[MsgType, StreamMsg],
    SyncParser[MsgType, StreamMsg],
]

SyncDecoder = Callable[
    [StreamMsg],
    DecodedMessage,
]
AsyncDecoder = Callable[
    [StreamMsg],
    Awaitable[DecodedMessage],
]
AsyncCustomDecoder = Union[
    AsyncDecoder[StreamMsg],
    Callable[
        [StreamMsg, AsyncDecoder[StreamMsg]],
        Awaitable[DecodedMessage],
    ],
]
Decoder = Union[
    AsyncDecoder[StreamMsg],
    SyncDecoder[StreamMsg],
]
CustomDecoder = Union[
    AsyncCustomDecoder[StreamMsg],
    SyncDecoder[StreamMsg],
]

P_HandlerParams = ParamSpec("P_HandlerParams")
T_HandlerReturn = TypeVar(
    "T_HandlerReturn",
    bound=Union[SendableMessage, Awaitable[SendableMessage]],
    covariant=True,
)


class AsyncPublisherProtocol(Protocol):
    """A protocol for an asynchronous publisher.

    Methods:
        publish(message: SendableMessage, correlation_id: Optional[str] = None, **kwargs: Any) -> Optional[SendableMessage]:
            Publishes a message asynchronously.

            Args:
                message: The message to be published.
                correlation_id: The correlation ID for the message (optional).
                **kwargs: Additional keyword arguments.

            Returns:
                The published message (optional).
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """

    async def publish(
        self,
        message: SendableMessage,
        correlation_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Optional[SendableMessage]:
        """Publishes a message.

        Args:
            message: The message to be published.
            correlation_id: Optional correlation ID for the message.
            **kwargs: Additional keyword arguments.

        Returns:
            The published message, or None if the message was not published.
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        ...


WrappedReturn = Tuple[T_HandlerReturn, Optional[AsyncPublisherProtocol]]

AsyncWrappedHandlerCall = Callable[
    [StreamMessage[MsgType]],
    Awaitable[Optional[WrappedReturn[T_HandlerReturn]]],
]
SyncWrappedHandlerCall = Callable[
    [StreamMessage[MsgType]],
    Optional[WrappedReturn[T_HandlerReturn]],
]
WrappedHandlerCall = Union[
    AsyncWrappedHandlerCall[MsgType, T_HandlerReturn],
    SyncWrappedHandlerCall[MsgType, T_HandlerReturn],
]
