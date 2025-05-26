from abc import abstractmethod
from collections.abc import AsyncIterator, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional

from typing_extensions import ReadOnly, Self

from faststream._internal.endpoint.usecase import Endpoint
from faststream._internal.types import MsgType

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.broker import BrokerConfig
    from faststream._internal.endpoint.publisher import BasePublisherProto
    from faststream._internal.endpoint.subscriber.call_item import HandlerItem
    from faststream._internal.producer import ProducerProto
    from faststream._internal.state import BrokerState, Pointer
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        SubscriberMiddleware,
    )
    from faststream.message import StreamMessage
    from faststream.response import Response


class SubscriberProto(Endpoint[MsgType]):
    calls: list["HandlerItem[MsgType]"]
    running: bool

    _producer: Optional["ProducerProto"]

    _broker_middlewares: ReadOnly[Sequence["BrokerMiddleware[MsgType]"]]

    def register(self, config: "BrokerConfig", /) -> None: ...

    @abstractmethod
    def get_log_context(
        self,
        msg: Optional["StreamMessage[MsgType]"],
        /,
    ) -> dict[str, str]: ...

    @abstractmethod
    def _setup(
        self,
        *,
        state: "Pointer[BrokerState]",
    ) -> None: ...

    @abstractmethod
    def _make_response_publisher(
        self,
        message: "StreamMessage[MsgType]",
    ) -> Iterable["BasePublisherProto"]: ...

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def consume(self, msg: MsgType) -> Any: ...

    @abstractmethod
    async def process_message(self, msg: MsgType) -> "Response": ...

    @abstractmethod
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "Optional[StreamMessage[MsgType]]": ...

    @abstractmethod
    def add_call(
        self,
        *,
        parser_: "CustomCallable",
        decoder_: "CustomCallable",
        middlewares_: Sequence["SubscriberMiddleware[Any]"],
        dependencies_: Iterable["Dependant"],
    ) -> Self: ...

    @abstractmethod
    def __aiter__(self) -> AsyncIterator["StreamMessage[MsgType]"]: ...
