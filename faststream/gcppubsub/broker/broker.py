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
from gcloud.aio.pubsub import PublisherClient, PubsubMessage, SubscriberClient
from typing_extensions import override

from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
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

    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.broker.registrator import Registrator
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict
    from faststream.types import SendableMessage


class GCPPubSubBroker(
    GCPPubSubRegistrator,
    BrokerUsecase[PubsubMessage, ConnectionState],
):
    """GCP Pub/Sub broker implementation."""

    def __init__(
        self,
        project_id: str,
        *,
        service_file: str | None = None,
        emulator_host: str | None = None,
        session: aiohttp.ClientSession | None = None,
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

        security_kwargs = parse_security(security) if security is not None else {}

        config = GCPPubSubBrokerConfig(
            producer=GCPPubSubFastProducer(
                project_id=project_id,
                service_file=service_file,
                emulator_host=emulator_host,
            ),
            project_id=project_id,
            connection=self._state,
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
                logger=logger
                if logger is not EMPTY and not isinstance(logger, object)
                else None,
                log_level=logging.INFO,
            )
            if logger is not EMPTY
            else make_gcppubsub_logger_state(
                logger=logging.getLogger(__name__),
                log_level=logging.INFO,
            ),
        )

        if schema is None:
            schema = BrokerSpec(
                description=description,
                url=[specification_url or "https://pubsub.googleapis.com"],
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

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        topic: str | None = None,
        *,
        attributes: dict[str, str] | None = None,
        ordering_key: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """Publish message to GCP Pub/Sub topic.

        Args:
            message: Message body to send
            topic: GCP Pub/Sub topic name
            attributes: Message attributes for metadata
            ordering_key: Message ordering key
            correlation_id: Manual correlation ID setter

        Returns:
            Published message ID
        """
        cmd = GCPPubSubPublishCommand(
            message,
            topic=topic or "",
            attributes=attributes,
            ordering_key=ordering_key,
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.PUBLISH,
        )

        result: str = await super()._basic_publish(
            cmd,
            producer=self.config.producer,
        )
        return result

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

        # Determine API root for emulator or production
        api_root = None
        if self.emulator_host:
            # Set environment variable for emulator
            import os

            os.environ["PUBSUB_EMULATOR_HOST"] = self.emulator_host
            # Set API root for gcloud-aio clients
            api_root = f"http://{self.emulator_host}/v1"

        # Create publisher client
        self._state.publisher = PublisherClient(
            service_file=self.service_file,
            session=session,
            api_root=api_root,
        )

        # Create subscriber client
        self._state.subscriber = SubscriberClient(
            service_file=self.service_file,
            session=session,
            api_root=api_root,
        )

        # Update producer with clients
        if hasattr(self.config.producer, "_publisher"):
            self.config.producer._publisher = self._state.publisher
        if hasattr(self.config.producer, "_session"):
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
