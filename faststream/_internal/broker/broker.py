from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Generic, Optional

from fast_depends import Provider
from typing_extensions import Self

from faststream._internal.configs import BrokerConfigType
from faststream._internal.types import (
    BrokerMiddleware,
    ConnectionType,
    MsgType,
)

from .pub_base import BrokerPublishMixin
from .registrator import Registrator

if TYPE_CHECKING:
    from types import TracebackType

    from faststream._internal.configs import BrokerConfig
    from faststream._internal.context.repository import ContextRepo
    from faststream._internal.producer import ProducerProto
    from faststream.specification.schema import BrokerSpec


class BrokerUsecase(
    Registrator[MsgType, BrokerConfigType],
    BrokerPublishMixin[MsgType],
    Generic[MsgType, ConnectionType, BrokerConfigType],
):
    """Basic class for brokers-only.

    Extends `Registrator` by connection, publish and AsyncAPI behavior.
    """

    _connection: ConnectionType | None

    def __init__(
        self,
        *,
        config: BrokerConfigType,
        specification: "BrokerSpec",
        routers: Iterable[Registrator[Any, Any]],
        **connection_kwargs: Any,
    ) -> None:
        super().__init__(
            routers=routers,
            config=config,
        )
        self.specification = specification

        self.running = False

        self._connection_kwargs = connection_kwargs
        self._connection = None

    @property
    def middlewares(self) -> Sequence["BrokerMiddleware[MsgType]"]:
        return self.config.broker_middlewares

    @property
    def _producer(self) -> "ProducerProto":
        return self.config.producer

    @property
    def context(self) -> "ContextRepo":
        return self.config.fd_config.context

    @property
    def provider(self) -> Provider:
        return self.config.fd_config.provider

    async def __aenter__(self) -> "Self":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Optional["TracebackType"],
    ) -> None:
        await self.stop(exc_type, exc_val, exc_tb)

    def _update_config(self, config: "BrokerConfig") -> None:
        """Private method to change broker config state by outer application."""
        self.config.add_outer_config(config)

    async def start(self) -> None:
        # TODO: filter by already running handlers after TestClient refactor
        for sub in self.subscribers:
            await sub.start()

        for pub in self.publishers:
            await pub.start()

        self.running = True

    def _setup_logger(self) -> None:
        for sub in self.subscribers:
            log_context = sub.get_log_context(None)
            log_context.pop("message_id", None)
            self.config.logger.params_storage.register_subscriber(log_context)

        self.config.logger._setup(self.config.fd_config.context)

    async def connect(self) -> ConnectionType:
        """Connect to a remote server."""
        if self._connection is None:
            self._check_addresses()
            self._connection = await self._connect()
            self._setup_logger()

        return self._connection

    def _check_addresses(self) -> None:
        """Read every Subscriber's addresses once, before a message can arrive.

        This is the moment an address is first known in full — the Router prefix
        composed, any Config value resolved — and the last one before the endpoint
        starts consuming, so it is where an address that cannot deliver its
        `Path()` parameters is refused.
        """
        for sub in self.subscribers:
            sub.check_addresses()

    @abstractmethod
    async def _connect(self) -> ConnectionType:
        raise NotImplementedError

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        """Closes the object."""
        for sub in self.subscribers:
            await sub.stop()

        self.running = False

    @abstractmethod
    async def ping(self, timeout: float | None) -> bool:
        """Check connection alive."""
        raise NotImplementedError
