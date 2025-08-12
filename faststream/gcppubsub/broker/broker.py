"""GCP Pub/Sub broker implementation."""

import logging
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Union,
)

import aiohttp
from gcloud.aio.pubsub import PubsubMessage, PublisherClient, SubscriberClient
from typing_extensions import override

from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.di import FastDependsConfig
from faststream.gcppubsub.configs.broker import GCPPubSubBrokerConfig
from faststream.gcppubsub.configs.state import ConnectionState
from faststream.gcppubsub.publisher.producer import GCPPubSubFastProducer
from faststream.gcppubsub.response import GCPPubSubPublishCommand
from faststream.gcppubsub.security import parse_security
from faststream.message import gen_cor_id
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec

from .logging import make_gcppubsub_logger_state
from .registrator import GCPPubSubRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.broker.registrator import Registrator
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.gcppubsub.message import GCPPubSubMessage
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict


class GCPPubSubBroker(
    GCPPubSubRegistrator,
    BrokerUsecase[PubsubMessage, ConnectionState],
):
    """GCP Pub/Sub broker implementation."""

    def __init__(
        self,
        project_id: str,
        *,
        service_file: Optional[str] = None,
        emulator_host: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
        # Publisher settings
        publisher_max_messages: int = 100,
        publisher_max_bytes: int = 1024 * 1024,
        publisher_max_latency: float = 0.01,
        # Subscriber settings
        subscriber_max_messages: int = 1000,
        subscriber_ack_deadline: int = 600,
        subscriber_max_extension: int = 600,
        # Retry settings
        retry_max_attempts: int = 5,
        retry_max_delay: float = 60.0,
        retry_multiplier: float = 2.0,
        retry_min_delay: float = 1.0,
        # Broker base args
        graceful_timeout: float | None = None,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Sequence["Registrator[PubsubMessage]"] = (),
        # AsyncAPI args
        security: Optional["BaseSecurity"] = None,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = None,
        tags: Sequence[Union["Tag", "TagDict"]] | None = None,
        logger: Union["LoggerProto", object, None] = EMPTY,
        setup_state: bool = True,
        on_startup: Sequence[Any] = (),
        on_shutdown: Sequence[Any] = (),
        # Specification
        schema_generator_name: str | None = None,
        description: str | None = None,
        schema: Optional["BrokerSpec"] = None,
        run_asgi_app: bool = False,
        asgi_app: Any | None = None,
    ) -> None:
        """Initialize GCP Pub/Sub broker.
        
        Args:
            project_id: GCP project ID
            service_file: Path to service account JSON file
            emulator_host: Pub/Sub emulator host (for testing)
            session: Existing aiohttp session to reuse
            publisher_max_messages: Max messages per batch
            publisher_max_bytes: Max bytes per batch
            publisher_max_latency: Max latency before publishing batch
            subscriber_max_messages: Max messages to pull
            subscriber_ack_deadline: ACK deadline in seconds
            subscriber_max_extension: Max ACK deadline extension
            retry_max_attempts: Max retry attempts
            retry_max_delay: Max retry delay in seconds
            retry_multiplier: Retry delay multiplier
            retry_min_delay: Min retry delay in seconds
            graceful_timeout: Graceful shutdown timeout
            decoder: Message decoder
            parser: Message parser
            dependencies: Broker dependencies
            middlewares: Broker middlewares
            routers: Message routers
            security: Security configuration
            specification_url: AsyncAPI specification URL
            protocol: Protocol name
            protocol_version: Protocol version
            tags: AsyncAPI tags
            logger: Logger instance
            setup_state: Whether to setup logging state
            on_startup: Startup hooks
            on_shutdown: Shutdown hooks
            schema_generator_name: Schema generator name
            description: Broker description
            schema: Broker specification
            run_asgi_app: Whether to run ASGI app
            asgi_app: ASGI application
        """
        self.project_id = project_id
        self.service_file = service_file
        self.emulator_host = emulator_host
        self._provided_session = session
        self._state = ConnectionState()
        
        if security is not None:
            security_kwargs = parse_security(security)
        else:
            security_kwargs = {}
        
        config = GCPPubSubBrokerConfig(
            producer=GCPPubSubFastProducer(
                project_id=project_id,
                service_file=service_file,
                emulator_host=emulator_host,
            ),
            project_id=project_id,
            service_file=service_file,
            emulator_host=emulator_host,
            session=session,
            publisher_max_messages=publisher_max_messages,
            publisher_max_bytes=publisher_max_bytes,
            publisher_max_latency=publisher_max_latency,
            subscriber_max_messages=subscriber_max_messages,
            subscriber_ack_deadline=subscriber_ack_deadline,
            subscriber_max_extension=subscriber_max_extension,
            retry_max_attempts=retry_max_attempts,
            retry_max_delay=retry_max_delay,
            retry_multiplier=retry_multiplier,
            retry_min_delay=retry_min_delay,
            # both args
            broker_middlewares=middlewares,
            broker_parser=parser,
            broker_decoder=decoder,
            logger=make_gcppubsub_logger_state(
                logger=logger,
                log_level=logging.INFO,
            ) if logger is not EMPTY else make_gcppubsub_logger_state(
                logger=logging.getLogger(__name__),
                log_level=logging.INFO,
            ),
        )
        
        if schema is None:
            schema = BrokerSpec(
                description=description,
                url=[specification_url or f"https://pubsub.googleapis.com"],
                protocol=protocol or "gcppubsub",
                protocol_version=protocol_version or "1.0",
                security=security,
                tags=tags or [],
            )
        
        super().__init__(
            config=config,
            specification=schema,
            routers=routers,
            **security_kwargs,
        )
        
        self._on_startup_hooks = list(on_startup)
        self._on_shutdown_hooks = list(on_shutdown)
        
        self.run_asgi_app = run_asgi_app
        self.asgi_app = asgi_app
    
    @override
    async def _connect(self) -> ConnectionState:
        """Connect to GCP Pub/Sub."""
        if self._provided_session:
            session = self._provided_session
            owns_session = False
        else:
            session = aiohttp.ClientSession()
            owns_session = True
        
        self._state.session = session
        self._state.owns_session = owns_session
        
        # Create publisher client
        self._state.publisher = PublisherClient(
            project=self.project_id,
            service_file=self.service_file,
            session=session,
        )
        
        # Create subscriber client
        self._state.subscriber = SubscriberClient(
            project=self.project_id,
            service_file=self.service_file,
            session=session,
        )
        
        # Set emulator host if provided
        if self.emulator_host:
            import os
            os.environ["PUBSUB_EMULATOR_HOST"] = self.emulator_host
        
        # Update producer with clients
        self.config.producer._publisher = self._state.publisher
        self.config.producer._session = session
        
        return self._state
    
    @override
    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        """Close the broker connection."""
        await super().stop(exc_type, exc_val, exc_tb)
        
        if self._connection:
            await self._state.close()
            self._connection = None