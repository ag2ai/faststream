from abc import abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
)

from typing_extensions import Self, deprecated, override

from faststream.asyncapi.proto import AsyncAPIProto
from faststream.broker.proto import EndpointProto
from faststream.broker.types import MsgType
from faststream.broker.wrapper.proto import WrapperProto

if TYPE_CHECKING:
    from fast_depends.dependencies import Depends

    from faststream.broker.message import StreamMessage
    from faststream.broker.publisher.proto import BasePublisherProto, ProducerProto
    from faststream.broker.response import Response
    from faststream.broker.subscriber.call_item import HandlerItem
    from faststream.broker.types import (
        BrokerMiddleware,
        CustomCallable,
        Filter,
        SubscriberMiddleware,
    )
    from faststream.types import AnyDict, Decorator, LoggerProto


class SubscriberProto(
    AsyncAPIProto,
    EndpointProto,
    WrapperProto[MsgType],
):
    calls: List["HandlerItem[MsgType]"]
    running: bool

    _broker_dependencies: Iterable["Depends"]
    _broker_middlewares: Sequence["BrokerMiddleware[MsgType]"]
    _producer: Optional["ProducerProto"]

    @abstractmethod
    def add_middleware(self, middleware: "BrokerMiddleware[MsgType]") -> None: ...

    @abstractmethod
    def get_log_context(
        self,
        msg: Optional["StreamMessage[MsgType]"],
        /,
    ) -> Dict[str, str]: ...

    @override
    @abstractmethod
    def setup(  # type: ignore[override]
        self,
        *,
        logger: Optional["LoggerProto"],
        graceful_timeout: Optional[float],
        broker_parser: Optional["CustomCallable"],
        broker_decoder: Optional["CustomCallable"],
        producer: Optional["ProducerProto"],
        extra_context: "AnyDict",
        # FastDepends options
        apply_types: bool,
        is_validate: bool,
        _get_dependant: Optional[Callable[..., Any]],
        _call_decorators: Iterable["Decorator"],
    ) -> None: ...

    @abstractmethod
    def _make_response_publisher(
        self,
        message: "StreamMessage[MsgType]",
    ) -> Iterable["BasePublisherProto"]: ...

    @property
    @abstractmethod
    def call_name(self) -> str: ...

    @abstractmethod
    async def start(self) -> None: ...

    @deprecated(
        "Deprecated in **FastStream 0.5.44**. "
        "Please, use `stop` method instead. "
        "Method `close` will be removed in **FastStream 0.7.0**.",
        category=DeprecationWarning,
        stacklevel=1,
    )
    async def close(self) -> None:
        return await self.stop()

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def consume(self, msg: MsgType) -> Any: ...

    @abstractmethod
    async def process_message(self, msg: MsgType) -> "Response": ...

    @abstractmethod
    async def get_one(
        self, *, timeout: float = 5.0
    ) -> "Optional[StreamMessage[MsgType]]": ...

    @abstractmethod
    def add_call(
        self,
        *,
        filter_: "Filter[Any]",
        parser_: "CustomCallable",
        decoder_: "CustomCallable",
        middlewares_: Sequence["SubscriberMiddleware[Any]"],
        dependencies_: Iterable["Depends"],
    ) -> Self: ...
