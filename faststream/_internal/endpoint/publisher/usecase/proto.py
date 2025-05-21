from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Protocol,
)

from faststream._internal.endpoint.usecase import Endpoint
from faststream._internal.types import (
    MsgType,
)
from faststream.response.response import PublishCommand

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.producer import ProducerProto
    from faststream._internal.state import BrokerState, Pointer
    from faststream._internal.types import (
        BrokerMiddleware,
        PublisherMiddleware,
    )
    from faststream.response.response import PublishCommand


class BasePublisherProto(Protocol):
    @abstractmethod
    async def publish(
        self,
        message: "SendableMessage",
        /,
        *,
        correlation_id: Optional[str] = None,
    ) -> Optional[Any]:
        """Public method to publish a message.

        Should be called by user only `broker.publisher(...).publish(...)`.
        """
        ...

    @abstractmethod
    async def _publish(
        self,
        cmd: "PublishCommand",
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """Private method to publish a message.

        Should be called inside `publish` method or as a step of `consume` scope.
        """
        ...

    @abstractmethod
    async def request(
        self,
        message: "SendableMessage",
        /,
        *,
        correlation_id: Optional[str] = None,
    ) -> Optional[Any]:
        """Publishes a message synchronously."""
        ...


class PublisherProto(
    Endpoint[MsgType],
    BasePublisherProto,
):
    _broker_middlewares: Sequence["BrokerMiddleware[MsgType]"]
    _middlewares: Sequence["PublisherMiddleware"]

    @property
    @abstractmethod
    def _producer(self) -> "ProducerProto": ...

    @abstractmethod
    def add_middleware(self, middleware: "BrokerMiddleware[MsgType]") -> None: ...

    @abstractmethod
    def _setup(
        self,
        *,
        state: "Pointer[BrokerState]",
        producer: "ProducerProto",
    ) -> None: ...
