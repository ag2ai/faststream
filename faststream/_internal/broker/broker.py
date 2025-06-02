from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Generic,
    Optional,
    Union,
)

from fast_depends import Provider
from typing_extensions import Doc, Self

from faststream._internal.types import (
    BrokerMiddleware,
    ConnectionType,
    MsgType,
)
from faststream.specification.proto import ServerSpecification

from .abc_broker import ABCBroker
from .pub_base import BrokerPublishMixin

if TYPE_CHECKING:
    from types import TracebackType

    from faststream._internal.context.repository import ContextRepo
    from faststream._internal.di import FastDependsConfig
    from faststream._internal.producer import ProducerProto
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict

    from .config import BrokerConfig


class BrokerUsecase(
    ABCBroker[MsgType],
    ServerSpecification,
    BrokerPublishMixin[MsgType],
    Generic[MsgType, ConnectionType],
):
    """Basic class for brokers-only.

    Extends `ABCBroker` by connection, publish and AsyncAPI behavior.
    """

    url: Union[str, list[str]]
    _connection: Optional[ConnectionType]

    def __init__(
        self,
        *,
        config: "BrokerConfig",
        routers: Annotated[
            Sequence["ABCBroker[MsgType]"],
            Doc("Routers to apply to broker."),
        ],
        # AsyncAPI kwargs
        protocol: Annotated[
            Optional[str],
            Doc("AsyncAPI server protocol."),
        ],
        protocol_version: Annotated[
            Optional[str],
            Doc("AsyncAPI server protocol version."),
        ],
        description: Annotated[
            Optional[str],
            Doc("AsyncAPI server description."),
        ],
        tags: Annotated[
            Iterable[Union["Tag", "TagDict"]],
            Doc("AsyncAPI server tags."),
        ],
        specification_url: Annotated[
            Union[str, list[str]],
            Doc("AsyncAPI hardcoded server addresses."),
        ],
        security: Annotated[
            Optional["BaseSecurity"],
            Doc(
                "Security options to connect broker and generate AsyncAPI server security.",
            ),
        ],
        **connection_kwargs: Any,
    ) -> None:
        super().__init__(
            routers=routers,
            config=config,
        )

        self.running = False

        self._connection_kwargs = connection_kwargs
        self._connection = None

        # AsyncAPI information
        self.url = specification_url
        self.protocol = protocol
        self.protocol_version = protocol_version
        self.description = description
        self.tags = tags
        self.security = security

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
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional["TracebackType"],
    ) -> None:
        await self.close(exc_type, exc_val, exc_tb)

    def _update_fd_config(self, config: "FastDependsConfig") -> None:
        """Private method to change broker config state by outer application."""
        self.config.broker_config.fd_config = (
            config | self.config.broker_config.fd_config
        )

    async def start(self) -> None:
        self._setup_logger()

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

        self.config.logger._setup(context=self.config.fd_config.context)

    async def connect(self) -> ConnectionType:
        """Connect to a remote server."""
        if self._connection is None:
            self._connection = await self._connect()
        return self._connection

    @abstractmethod
    async def _connect(self) -> ConnectionType:
        raise NotImplementedError

    async def close(
        self,
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        """Closes the object."""
        for sub in self.subscribers:
            await sub.close()

        self.running = False

    @abstractmethod
    async def ping(self, timeout: Optional[float]) -> bool:
        """Check connection alive."""
        raise NotImplementedError
