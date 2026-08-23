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
            self._prepare()
            self._connection = await self._connect()

        return self._connection

    def _prepare(self) -> None:
        """Preparation: everything derivable from the options composition, no I/O.

        The moment the composition is final — the Router prefix composed, the
        Config values in scope — and the last one before anything talks to the
        network. Every static step lives here so that a declaration mistake
        refuses the Broker rather than aborting a start-up already under way.

        Endpoints first and the logger second: logger setup reads every
        Subscriber's log context, which reads their resolved addresses.

        Synchronous and idempotent, so an App can drive it across all its Brokers
        before connecting any, and schema generation can drive it with no event
        loop at all. No flag guards this method: the endpoints carry their own,
        and both logger steps already settle — registering a log context widens a
        column width, and the logger object is built only if there is not one.
        The flag it does keep records that this happened, for the sake of an
        endpoint attached afterwards.
        """
        for sub in self.subscribers:
            sub.prepare()

        for pub in self.publishers:
            pub.prepare()

        self._setup_logger()

        self._prepared = True

    def _invalidate(self) -> None:
        """Undo Preparation across every endpoint.

        The counterpart of `_prepare`, driven where the connection is cleared,
        so that a stopped Broker prepares again on its next `connect()` — which
        is what "a Config value is fixed at `connect()`" (ADR-0004) means for a
        Broker used twice.
        """
        self._prepared = False

        for sub in self.subscribers:
            sub.invalidate()

        for pub in self.publishers:
            pub.invalidate()

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

        # After the Subscribers have stopped reading through their addresses,
        # and before the next `connect()` derives them again.
        self._invalidate()

    @abstractmethod
    async def ping(self, timeout: float | None) -> bool:
        """Check connection alive."""
        raise NotImplementedError
