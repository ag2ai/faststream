from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Protocol

from faststream._internal.broker import BrokerUsecase

from .specification import Specification

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerUsecase
    from faststream.asgi.handlers import HttpHandler


class SpecificationFactory(Protocol):
    @abstractmethod
    def add_broker(
        self,
        broker: "BrokerUsecase[Any, Any]",
        /,
    ) -> "SpecificationFactory":
        raise NotImplementedError

    @abstractmethod
    def add_http_route(
        self,
        path: str,
        handler: "HttpHandler",
    ) -> "SpecificationFactory":
        raise NotImplementedError

    @abstractmethod
    def to_specification(self) -> Specification:
        raise NotImplementedError
