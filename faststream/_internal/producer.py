from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Protocol

from typing_extensions import ReadOnly

from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from faststream._internal.types import AsyncCallable
    from faststream.response import PublishCommand


class ProducerProto(Protocol):
    _parser: ReadOnly["AsyncCallable"]
    _decoder: ReadOnly["AsyncCallable"]

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


class ProducerUnset(ProducerProto):
    msg = "Producer is unset yet. You should set producer in broker initial method."

    def __bool__(self) -> bool:
        return False

    @property
    def _decoder(self) -> "AsyncCallable":
        raise IncorrectState(self.msg)

    def _parser(self) -> "AsyncCallable":
        raise IncorrectState(self.msg)

    async def publish(self, cmd: "PublishCommand") -> Optional[Any]:
        raise IncorrectState(self.msg)

    async def request(self, cmd: "PublishCommand") -> Any:
        raise IncorrectState(self.msg)

    async def publish_batch(self, cmd: "PublishCommand") -> None:
        raise IncorrectState(self.msg)
