from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
)

from faststream._internal.endpoint.usecase import Endpoint
from faststream._internal.types import MsgType

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import PublisherMiddleware
    from faststream.response import PublishCommand


class BasePublisherProto(Protocol):
    @abstractmethod
    async def publish(
        self,
        message: "SendableMessage",
        /,
        *,
        correlation_id: str | None = None,
    ) -> Any | None:
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
        correlation_id: str | None = None,
    ) -> Any | None:
        """Publishes a message synchronously."""
        ...


class PublisherProto(Endpoint[MsgType], BasePublisherProto):
    _middlewares: Sequence["PublisherMiddleware"]

    @abstractmethod
    async def start(self) -> None: ...
