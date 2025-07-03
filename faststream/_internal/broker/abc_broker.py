from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Generic,
)

from faststream._internal.configs import BrokerConfig, ConfigComposition
from faststream._internal.endpoint.publisher import PublisherProto
from faststream._internal.endpoint.subscriber import (
    SubscriberProto,
)
from faststream._internal.types import BrokerMiddleware, MsgType

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant


class FinalSubscriber(
    SubscriberProto[MsgType],
):
    @property
    @abstractmethod
    def call_name(self) -> str:
        raise NotImplementedError


class FinalPublisher(
    PublisherProto[MsgType],
):
    pass


class Registrator(Generic[MsgType]):
    """Basic class for brokers and routers.

    Contains subscribers & publishers registration logic only.
    """

    def __init__(
        self,
        *,
        config: "BrokerConfig",
        routers: Sequence["Registrator[MsgType]"],
    ) -> None:
        self.config = ConfigComposition(config)
        self._parser = self.config.broker_parser
        self._decoder = self.config.broker_decoder

        self._subscribers: list[FinalSubscriber[MsgType]] = []
        self._publishers: list[FinalPublisher[MsgType]] = []
        self.routers: list[Registrator] = []

        self.include_routers(*routers)

    @property
    def subscribers(self) -> list[FinalSubscriber[MsgType]]:
        return self._subscribers + [sub for r in self.routers for sub in r.subscribers]

    @property
    def publishers(self) -> list[FinalPublisher[MsgType]]:
        return self._publishers + [pub for r in self.routers for pub in r.publishers]

    def add_middleware(self, middleware: "BrokerMiddleware[MsgType]") -> None:
        """Append BrokerMiddleware to the end of middlewares list.

        Current middleware will be used as a most inner of the stack.
        """
        self.config.add_middleware(middleware)

    def insert_middleware(self, middleware: "BrokerMiddleware[MsgType]") -> None:
        """Insert BrokerMiddleware to the start of middlewares list.

        Current middleware will be used as a most outer of the stack.
        """
        self.config.insert_middleware(middleware)

    @abstractmethod
    def subscriber(
        self,
        subscriber: "FinalSubscriber[MsgType]",
    ) -> "FinalSubscriber[MsgType]":
        self._subscribers.append(subscriber)
        return subscriber

    @abstractmethod
    def publisher(
        self,
        publisher: "FinalPublisher[MsgType]",
    ) -> "FinalPublisher[MsgType]":
        self._publishers.append(publisher)
        return publisher

    def include_router(
        self,
        router: "Registrator[MsgType]",
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Iterable["BrokerMiddleware[MsgType]"] = (),
        include_in_schema: bool | None = None,
    ) -> None:
        """Includes a router in the current object."""
        if options_config := BrokerConfig(
            prefix=prefix,
            include_in_schema=include_in_schema,
            broker_middlewares=middlewares,
            broker_dependencies=dependencies,
        ):
            router.config.add_config(options_config)

        router.config.add_config(self.config)
        self.routers.append(router)

    def include_routers(
        self,
        *routers: "Registrator[MsgType]",
    ) -> None:
        """Includes routers in the object."""
        for r in routers:
            self.include_router(r)
