from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Protocol,
)

from faststream._internal.proto import Endpoint
from faststream._internal.types import (
    MsgType,
)
from faststream.response.response import PublishCommand

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.state import BrokerState, Pointer
    from faststream._internal.types import (
        AsyncCallable,
        BrokerMiddleware,
        PublisherMiddleware,
    )
    from faststream.response.response import PublishCommand


class ProducerProto(Protocol):
    _parser: "AsyncCallable"
    _decoder: "AsyncCallable"

    @abstractmethod
    async def publish(self, cmd: "PublishCommand") -> Optional[Any]:
        """Publishes a message asynchronously."""
        ...

    @abstractmethod
    async def request(self, cmd: "PublishCommand") -> Any:
        """Publishes a message synchronously."""
        ...

    @abstractmethod
    async def publish_batch(self, cmd: "PublishCommand") -> Any:
        """Publishes a messages batch asynchronously."""
        ...


class ProducerFactory(Protocol):
    def __call__(
        self, parser: "AsyncCallable", decoder: "AsyncCallable"
    ) -> ProducerProto: ...


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
