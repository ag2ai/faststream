from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Generic
from weakref import WeakSet

from faststream._internal.configs import BrokerConfig, BrokerConfigType, ConfigComposition
from faststream._internal.types import BrokerMiddleware, MsgType

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.endpoint.publisher import PublisherUsecase
    from faststream._internal.endpoint.subscriber import SubscriberUsecase


class Registrator(Generic[MsgType, BrokerConfigType]):
    """Basic class for brokers and routers.

    Contains subscribers & publishers registration logic only.
    """

    def __init__(
        self,
        *,
        config: BrokerConfigType,
        routers: Iterable["Registrator[MsgType]"],
    ) -> None:
        self._parser = config.broker_parser
        self._decoder = config.broker_decoder

        self.config: ConfigComposition[BrokerConfigType] = ConfigComposition(config)

        self._subscribers: WeakSet[SubscriberUsecase[MsgType]] = WeakSet()
        self._publishers: WeakSet[PublisherUsecase] = WeakSet()
        self.routers: list[Registrator[MsgType, Any]] = []

        self.__persistent_subscribers: list[SubscriberUsecase[MsgType]] = []
        self.__persistent_publishers: list[PublisherUsecase] = []

        self.__parent: Registrator[MsgType, Any] | None = None

        self.include_routers(*routers)

    @property
    def subscribers(self) -> list["SubscriberUsecase[MsgType]"]:
        return [*self._subscribers, *(sub for r in self.routers for sub in r.subscribers)]

    @property
    def publishers(self) -> list["PublisherUsecase"]:
        return [*self._publishers, *(pub for r in self.routers for pub in r.publishers)]

    def add_middleware(self, middleware: "BrokerMiddleware[Any, Any]") -> None:
        """Append BrokerMiddleware to the end of middlewares list.

        Current middleware will be used as a most inner of the stack.
        """
        self.config.add_middleware(middleware)

    def insert_middleware(self, middleware: "BrokerMiddleware[Any, Any]") -> None:
        """Insert BrokerMiddleware to the start of middlewares list.

        Current middleware will be used as a most outer of the stack.
        """
        self.config.insert_middleware(middleware)

    @abstractmethod
    def subscriber(
        self,
        subscriber: "SubscriberUsecase[MsgType]",
        persistent: bool = True,
    ) -> "SubscriberUsecase[MsgType]":
        self._subscribers.add(subscriber)
        if persistent:
            self.__persistent_subscribers.append(subscriber)
        return subscriber

    @abstractmethod
    def publisher(
        self,
        publisher: "PublisherUsecase",
        persistent: bool = True,
    ) -> "PublisherUsecase":
        self._publishers.add(publisher)
        if persistent:
            self.__persistent_publishers.append(publisher)
        return publisher

    def include_router(
        self,
        router: "Registrator[MsgType, Any]",
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[MsgType]"] = (),
        include_in_schema: bool | None = None,
    ) -> None:
        """Includes a router in the current object."""
        if router.parent is self:
            return
        router.parent = self

        if options_config := BrokerConfig(
            prefix=prefix,
            include_in_schema=include_in_schema,
            broker_middlewares=middlewares,
            broker_dependencies=dependencies,
        ):
            router.config.add_config(options_config)

        router.config.add_config(self.config)
        self.routers.append(router)

    @property
    def parent(self) -> "Registrator[MsgType, Any] | None":
        return self.__parent

    @parent.setter
    def parent(self, parent: "Registrator[MsgType, Any]") -> None:
        if self.__parent is not None and parent is not self.__parent:
            self.__parent.routers.remove(self)
            self.config.reset()
        self.__parent = parent

    def include_routers(
        self,
        *routers: "Registrator[MsgType, Any]",
    ) -> None:
        """Includes routers in the object."""
        for r in routers:
            self.include_router(r)
