from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    Generic,
    Optional,
    Union,
    cast,
)

from fast_depends import Provider
from typing_extensions import Doc, Self

from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.state import (
    DIState,
    LoggerState,
    SetupAble,
)
from faststream._internal.state.broker import (
    BrokerState,
    InitialBrokerState,
)
from faststream._internal.state.producer import ProducerUnset
from faststream._internal.subscriber.proto import SubscriberProto
from faststream._internal.types import (
    AsyncCustomCallable,
    BrokerMiddleware,
    ConnectionType,
    CustomCallable,
    MsgType,
)
from faststream._internal.utils.functions import to_async
from faststream.specification.proto import ServerSpecification

from .abc_broker import ABCBroker, FinalPublisher
from .pub_base import BrokerPublishMixin

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import AnyDict, Decorator
    from faststream._internal.publisher.proto import ProducerProto
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict


class BrokerUsecase(
    ABCBroker[MsgType],
    SetupAble,
    ServerSpecification,
    BrokerPublishMixin[MsgType],
    Generic[MsgType, ConnectionType],
):
    """A class representing a broker async use case."""

    url: Union[str, list[str]]
    _connection: Optional[ConnectionType]
    middlewares: Sequence["BrokerMiddleware[MsgType]"]

    def __init__(
        self,
        *,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Custom decoder object."),
        ],
        parser: Annotated[
            Optional["CustomCallable"],
            Doc("Custom parser object."),
        ],
        dependencies: Annotated[
            Iterable["Dependant"],
            Doc("Dependencies to apply to all broker subscribers."),
        ],
        middlewares: Annotated[
            Sequence["BrokerMiddleware[MsgType]"],
            Doc("Middlewares to apply to all broker publishers/subscribers."),
        ],
        graceful_timeout: Annotated[
            Optional[float],
            Doc(
                "Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down.",
            ),
        ],
        routers: Annotated[
            Sequence["ABCBroker[MsgType]"],
            Doc("Routers to apply to broker."),
        ],
        # Logging args
        logger_state: LoggerState,
        # FastDepends args
        apply_types: Annotated[
            bool,
            Doc("Whether to use FastDepends or not."),
        ],
        serializer: Optional["SerializerProto"] = EMPTY,
        _get_dependant: Annotated[
            Optional[Callable[..., Any]],
            Doc("Custom library dependant generator callback."),
        ],
        _call_decorators: Annotated[
            Sequence["Decorator"],
            Doc("Any custom decorator to apply to wrapped functions."),
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
        state = InitialBrokerState(
            di_state=DIState(
                use_fastdepends=apply_types,
                get_dependent=_get_dependant,
                call_decorators=_call_decorators,
                serializer=serializer,
                provider=Provider(),
                context=ContextRepo(),
            ),
            logger_state=logger_state,
            graceful_timeout=graceful_timeout,
            producer=ProducerUnset(),
        )

        super().__init__(
            middlewares=middlewares,
            dependencies=dependencies,
            decoder=cast(
                "Optional[AsyncCustomCallable]",
                to_async(decoder) if decoder else None,
            ),
            parser=cast(
                "Optional[AsyncCustomCallable]",
                to_async(parser) if parser else None,
            ),
            routers=routers,
            # Broker is a root router
            include_in_schema=True,
            prefix="",
            state=state,
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
    def _producer(self) -> "ProducerProto":
        return self._state.get().producer

    @property
    def context(self) -> "ContextRepo":
        return self._state.get().di_state.context

    @property
    def provider(self) -> Provider:
        return self._state.get().di_state.provider

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

    @abstractmethod
    async def start(self) -> None:
        """Start the broker async use case."""
        # TODO: filter by already running handlers after TestClient refactor
        state = self._state.get()

        for subscriber in self._subscribers:
            state.logger_state.log(
                f"`{subscriber.call_name}` waiting for messages",
                extra=subscriber.get_log_context(None),
            )
            await subscriber.start()

        if not self.running:
            self.running = True

    def _setup_logger_state(self) -> None:
        state = self._state.get()

        for subscriber in self._subscribers:
            log_context = subscriber.get_log_context(None)
            log_context.pop("message_id", None)
            state.logger_state.params_storage.setup_log_contest(log_context)

        state._setup_logger_state()

    async def connect(self, **kwargs: Any) -> ConnectionType:
        """Connect to a remote server."""
        if self._connection is None:
            connection_kwargs = self._connection_kwargs.copy()
            connection_kwargs.update(kwargs)
            self._connection = await self._connect(**connection_kwargs)

        return self._connection

    @abstractmethod
    async def _connect(self) -> ConnectionType:
        """Connect to a resource."""
        raise NotImplementedError

    def _setup(self, state: Optional["BrokerState"] = None) -> None:
        """Prepare all Broker entities to startup.

        Method should be idempotent due could be called twice
        """
        broker_state = self._state.get()
        current_di_state = broker_state.di_state
        broker_serializer = current_di_state.serializer

        self._setup_logger_state()

        if state is not None:
            di_state = state.di_state

            if broker_serializer is EMPTY:
                broker_serializer = di_state.serializer

            current_di_state.update(
                serializer=broker_serializer,
                provider=di_state.provider,
                context=di_state.context,
            )

        else:
            # Fallback to default state if there no
            # parent container like FastStream object
            if broker_serializer is EMPTY:
                from fast_depends.pydantic import PydanticSerializer

                broker_serializer = PydanticSerializer()

            current_di_state.update(
                serializer=broker_serializer,
            )

        broker_state._setup()

        # TODO: move setup to object creation
        for h in self._subscribers:
            self.setup_subscriber(h)

        for p in self._publishers:
            self.setup_publisher(p)

    def setup_subscriber(
        self,
        subscriber: SubscriberProto[MsgType],
        **kwargs: Any,
    ) -> None:
        """Setup the Subscriber to prepare it to starting."""
        data = self._subscriber_setup_extra.copy()
        data.update(kwargs)
        subscriber._setup(**data, state=self._state)

    @property
    def _subscriber_setup_extra(self) -> "AnyDict":
        return {
            "extra_context": {
                "broker": self,
            },
            # broker options
            "broker_parser": self._parser,
            "broker_decoder": self._decoder,
        }

    def publisher(
        self,
        publisher: "FinalPublisher[MsgType]",
        is_running: bool = False,
    ) -> "FinalPublisher[MsgType]":
        pub = super().publisher(publisher, is_running=self.running)
        self.setup_publisher(pub)
        return pub

    async def close(
        self,
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        """Closes the object."""
        for h in self._subscribers:
            await h.close()

        self.running = False

    @abstractmethod
    async def ping(self, timeout: Optional[float]) -> bool:
        """Check connection alive."""
        raise NotImplementedError
