from abc import abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Protocol,
)

from faststream.response.response import PublishCommand

if TYPE_CHECKING:
    from faststream._internal.types import (
        AsyncCallable,
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
