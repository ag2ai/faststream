from abc import abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterable,
    Mapping,
    Optional,
    Sequence,
)

from faststream.broker.types import MsgType

if TYPE_CHECKING:
    from fast_depends.dependencies import Depends

    from faststream.broker.publisher.proto import PublisherProto
    from faststream.broker.subscriber.proto import SubscriberProto
    from faststream.broker.types import (
        BrokerMiddleware,
        CustomCallable,
    )


class ABCBroker(Generic[MsgType]):
    _subscribers: Mapping[int, "SubscriberProto[MsgType]"]
    _publishers: Mapping[int, "PublisherProto[MsgType]"]

    def __init__(
        self,
        *,
        prefix: str,
        dependencies: Iterable["Depends"],
        middlewares: Sequence["BrokerMiddleware[MsgType]"],
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
        include_in_schema: Optional[bool],
    ) -> None:
        self.prefix = prefix
        self.include_in_schema = include_in_schema

        self._subscribers = {}
        self._publishers = {}

        self._dependencies = dependencies
        self._middlewares = middlewares
        self._parser = parser
        self._decoder = decoder

    def add_middleware(self, middleware: "BrokerMiddleware[MsgType]") -> None:
        """Append BrokerMiddleware to the end of middlewares list.

        Current middleware will be used as a most inner of already existed ones.
        """
        self._middlewares = (*self._middlewares, middleware)

        for sub in self._subscribers.values():
            sub.add_middleware(middleware)

        for pub in self._publishers.values():
            pub.add_middleware(middleware)

    @abstractmethod
    def subscriber(
        self,
        subscriber: "SubscriberProto[MsgType]",
    ) -> "SubscriberProto[MsgType]":
        subscriber.add_prefix(self.prefix)
        key = hash(subscriber)
        subscriber = self._subscribers.get(key, subscriber)
        self._subscribers = {**self._subscribers, key: subscriber}
        return subscriber

    @abstractmethod
    def publisher(
        self,
        publisher: "PublisherProto[MsgType]",
    ) -> "PublisherProto[MsgType]":
        publisher.add_prefix(self.prefix)
        key = hash(publisher)
        publisher = self._publishers.get(key, publisher)
        self._publishers = {**self._publishers, key: publisher}
        return publisher

    def include_router(
        self,
        router: "ABCBroker[Any]",
        *,
        prefix: str = "",
        dependencies: Iterable["Depends"] = (),
        middlewares: Iterable["BrokerMiddleware[MsgType]"] = (),
        include_in_schema: Optional[bool] = None,
    ) -> None:
        """Includes a router in the current object."""
        for h in router._subscribers.values():
            h.add_prefix("".join((self.prefix, prefix)))

            if (key := hash(h)) not in self._subscribers:
                if include_in_schema is None:
                    h.include_in_schema = self._solve_include_in_schema(
                        h.include_in_schema
                    )
                else:
                    h.include_in_schema = include_in_schema

                h._broker_middlewares = (
                    *self._middlewares,
                    *middlewares,
                    *h._broker_middlewares,
                )
                h._broker_dependencies = (
                    *self._dependencies,
                    *dependencies,
                    *h._broker_dependencies,
                )
                self._subscribers = {**self._subscribers, key: h}

        for p in router._publishers.values():
            p.add_prefix("".join((self.prefix, prefix)))

            if (key := hash(p)) not in self._publishers:
                if include_in_schema is None:
                    p.include_in_schema = self._solve_include_in_schema(
                        p.include_in_schema
                    )
                else:
                    p.include_in_schema = include_in_schema

                p._broker_middlewares = (
                    *self._middlewares,
                    *middlewares,
                    *p._broker_middlewares,
                )
                self._publishers = {**self._publishers, key: p}

    def include_routers(
        self,
        *routers: "ABCBroker[MsgType]",
    ) -> None:
        """Includes routers in the object."""
        for r in routers:
            self.include_router(r)

    def _solve_include_in_schema(self, include_in_schema: bool) -> bool:
        if self.include_in_schema is None or self.include_in_schema:
            return include_in_schema
        else:
            return self.include_in_schema
