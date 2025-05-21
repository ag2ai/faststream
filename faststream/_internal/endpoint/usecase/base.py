from abc import abstractmethod

from faststream._internal.endpoint.base import EndpointWrapper
from faststream._internal.types import MsgType


class Endpoint(EndpointWrapper[MsgType]):
    @abstractmethod
    def add_prefix(self, prefix: str) -> None: ...
