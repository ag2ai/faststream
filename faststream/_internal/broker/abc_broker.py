from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
)

from faststream._internal.publisher.proto import PublisherProto
from faststream._internal.state import BrokerState, Pointer
from faststream._internal.subscriber.proto import SubscriberProto
from faststream._internal.types import BrokerMiddleware, CustomCallable, MsgType
from faststream.specification.proto import EndpointSpecification
from faststream.specification.schema import PublisherSpec, SubscriberSpec

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant


class FinalSubscriber(
    EndpointSpecification[MsgType, SubscriberSpec],
    SubscriberProto[MsgType],
):
    @property
    @abstractmethod
    def call_name(self) -> str:
        raise NotImplementedError


class FinalPublisher(
    EndpointSpecification[MsgType, PublisherSpec],
    PublisherProto[MsgType],
):
    pass


class ABCBroker(Generic[MsgType]):
    _subscribers: list[FinalSubscriber[MsgType]]
    _publishers: list[FinalPublisher[MsgType]]

    def __init__(
        self,
        *,
        prefix: str,
        dependencies: Iterable["Dependant"],
        middlewares: Sequence["BrokerMiddleware[MsgType]"],
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
        include_in_schema: Optional[bool],
        state: "BrokerState",
        routers: Sequence["ABCBroker[MsgType]"],
    ) -> None:
        self.prefix = prefix
        self.include_in_schema = include_in_schema

        self._subscribers = []
        self._publishers = []

        self._dependencies = dependencies
        self.middlewares = middlewares
        self._parser = parser
        self._decoder = decoder

        self._state = Pointer(state)

        self.include_routers(*routers)

    def add_middleware(self, middleware: "BrokerMiddleware[MsgType]") -> None:
        """Append BrokerMiddleware to the end of middlewares list.

        Current middleware will be used as a most inner of already existed ones.
        """
        self.middlewares = (*self.middlewares, middleware)

        for sub in self._subscribers:
            sub.add_middleware(middleware)

        for pub in self._publishers:
            pub.add_middleware(middleware)

    @abstractmethod
    def subscriber(
        self,
        subscriber: "FinalSubscriber[MsgType]",
        is_running: bool = False,
    ) -> "FinalSubscriber[MsgType]":
        subscriber.add_prefix(self.prefix)
        if not is_running:
            self._subscribers.append(subscriber)
        return subscriber

    @abstractmethod
    def publisher(
        self,
        publisher: "FinalPublisher[MsgType]",
        is_running: bool = False,
    ) -> "FinalPublisher[MsgType]":
        publisher.add_prefix(self.prefix)
        if not is_running:
            self._publishers.append(publisher)
        return publisher

    def setup_publisher(
        self,
        publisher: "FinalPublisher[MsgType]",
        **kwargs: Any,
    ) -> None:
        """Setup the Publisher to prepare it to starting."""
        publisher._setup(**kwargs, state=self._state)

    def _setup(self, state: Optional["BrokerState"]) -> None:
        if state is not None:
            self._state.set(state)

    def include_router(
        self,
        router: "ABCBroker[Any]",
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Iterable["BrokerMiddleware[MsgType]"] = (),
        include_in_schema: Optional[bool] = None,
    ) -> None:
        """Includes a router in the current object."""
        router._setup(self._state.get())

        for h in router._subscribers:
            h.add_prefix(f"{self.prefix}{prefix}")

            if include_in_schema is None:
                h.include_in_schema = self._solve_include_in_schema(h.include_in_schema)
            else:
                h.include_in_schema = include_in_schema

            h._broker_middlewares = (
                *self.middlewares,
                *middlewares,
                *h._broker_middlewares,
            )
            h._broker_dependencies = (
                *self._dependencies,
                *dependencies,
                *h._broker_dependencies,
            )
            self._subscribers.append(h)

        for p in router._publishers:
            p.add_prefix(self.prefix)

            if include_in_schema is None:
                p.include_in_schema = self._solve_include_in_schema(p.include_in_schema)
            else:
                p.include_in_schema = include_in_schema

            p._broker_middlewares = (
                *self.middlewares,
                *middlewares,
                *p._broker_middlewares,
            )
            self._publishers.append(p)

    def include_routers(
        self,
        *routers: "ABCBroker[MsgType]",
    ) -> None:
        """Includes routers in the object."""
        for r in routers:
            self.include_router(r)

    def _solve_include_in_schema(self, include_in_schema: bool) -> bool:
        # should be `is False` to pass `None` case
        if self.include_in_schema is False:
            return False

        return include_in_schema
