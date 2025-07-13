from abc import abstractmethod
from collections.abc import Iterable
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
)

from faststream._internal.types import PublishCommandType_contra

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import PublisherMiddleware


class PublisherProto(Protocol[PublishCommandType_contra]):
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
        cmd: "PublishCommandType_contra",
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
