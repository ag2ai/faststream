from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.publisher.proto import ProducerProto
from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from faststream._internal.types import AsyncCallable
    from faststream.response import PublishCommand


class ProducerUnset(ProducerProto):
    msg = "Producer is unset yet. You should set producer in broker initial method."

    @property
    def _decoder(self) -> "AsyncCallable":
        raise IncorrectState(self.msg)

    @_decoder.setter
    def _decoder(self, value: "AsyncCallable", /) -> None:
        raise IncorrectState(self.msg)

    @property
    def _parser(self) -> "AsyncCallable":
        raise IncorrectState(self.msg)

    @_parser.setter
    def _parser(self, value: "AsyncCallable", /) -> None:
        raise IncorrectState(self.msg)

    async def publish(self, cmd: "PublishCommand") -> Optional[Any]:
        raise IncorrectState(self.msg)

    async def request(self, cmd: "PublishCommand") -> Any:
        raise IncorrectState(self.msg)

    async def publish_batch(self, cmd: "PublishCommand") -> None:
        raise IncorrectState(self.msg)
