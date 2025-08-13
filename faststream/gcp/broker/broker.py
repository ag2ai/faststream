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
from faststream._internal.di import FastDependsConfig
from faststream.gcp.configs import (
    GCPBrokerConfig,
    PublisherConfig,
    RetryConfig,
    SubscriberConfig,
)
from faststream.gcp.configs.state import ConnectionState
from faststream.gcp.publisher.producer import GCPFastProducer
from faststream.gcp.response import GCPPublishCommand
from faststream.gcp.security import parse_security
from faststream.message import gen_cor_id
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec

from .logging import make_gcp_logger_state
from .registrator import GCPRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.broker.registrator import Registrator
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream._internal.types.compat import SerializerProto
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict
    from faststream.types import SendableMessage


class GCPBroker(
    GCPRegistrator,
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
        # Configuration objects (new grouped approach)
        publisher_config: PublisherConfig | None = None,
        subscriber_config: SubscriberConfig | None = None,
        retry_config: RetryConfig | None = None,
        # Publisher settings (backward compatibility)
        publisher_max_messages: int | None = None,
        publisher_max_bytes: int | None = None,
        publisher_max_latency: float | None = None,
        # Subscriber settings (backward compatibility)
        subscriber_max_messages: int | None = None,
        subscriber_ack_deadline: int | None = None,
        subscriber_max_extension: int | None = None,
        # Retry settings (backward compatibility)
        retry_max_attempts: int | None = None,
        retry_max_delay: float | None = None,
        retry_multiplier: float | None = None,
        retry_min_delay: float | None = None,
        # Broker base args
        graceful_timeout: float | None = None,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Sequence["Registrator[PubsubMessage]"] = (),
        # FastDepends args
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
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
            publisher_config: Publisher configuration object
            subscriber_config: Subscriber configuration object
            retry_config: Retry configuration object
            publisher_max_messages: Max messages per batch (deprecated, use publisher_config)
            publisher_max_bytes: Max bytes per batch (deprecated, use publisher_config)
            publisher_max_latency: Max latency before publishing batch (deprecated, use publisher_config)
            subscriber_max_messages: Max messages to pull (deprecated, use subscriber_config)
            subscriber_ack_deadline: ACK deadline in seconds (deprecated, use subscriber_config)
            subscriber_max_extension: Max ACK deadline extension (deprecated, use subscriber_config)
            retry_max_attempts: Max retry attempts (deprecated, use retry_config)
            retry_max_delay: Max retry delay in seconds (deprecated, use retry_config)
            retry_multiplier: Retry delay multiplier (deprecated, use retry_config)
            retry_min_delay: Min retry delay in seconds (deprecated, use retry_config)
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
            apply_types: Whether to use FastDepends for type validation
            serializer: FastDepends-compatible serializer for message validation
        """
        self.project_id = project_id
        self.service_file = service_file
        self.emulator_host = emulator_host
        self._provided_session = session
        self._state = ConnectionState()

        security_kwargs = parse_security(security) if security is not None else {}

        # Handle configuration objects vs individual parameters
        # Publisher config
        if publisher_config is not None:
            final_publisher_config = publisher_config
        else:
            final_publisher_config = PublisherConfig(
                max_messages=publisher_max_messages
                if publisher_max_messages is not None
                else 100,
                max_bytes=publisher_max_bytes
                if publisher_max_bytes is not None
                else (1024 * 1024),
                max_latency=publisher_max_latency
                if publisher_max_latency is not None
                else 0.01,
            )

        # Subscriber config
        if subscriber_config is not None:
            final_subscriber_config = subscriber_config
        else:
            final_subscriber_config = SubscriberConfig(
                max_messages=subscriber_max_messages
                if subscriber_max_messages is not None
                else 1000,
                ack_deadline=subscriber_ack_deadline
                if subscriber_ack_deadline is not None
                else 600,
                max_extension=subscriber_max_extension
                if subscriber_max_extension is not None
                else 600,
            )

        # Retry config
        if retry_config is not None:
            final_retry_config = retry_config
        else:
            final_retry_config = RetryConfig(
                max_attempts=retry_max_attempts if retry_max_attempts is not None else 5,
                max_delay=retry_max_delay if retry_max_delay is not None else 60.0,
                multiplier=retry_multiplier if retry_multiplier is not None else 2.0,
                min_delay=retry_min_delay if retry_min_delay is not None else 1.0,
            )

        config = GCPBrokerConfig(
            producer=GCPFastProducer(
                project_id=project_id,
                service_file=service_file,
                emulator_host=emulator_host,
            ),
            project_id=project_id,
            connection=self._state,
            service_file=service_file,
            emulator_host=emulator_host,
            session=session,
            publisher=final_publisher_config,
            subscriber=final_subscriber_config,
            retry=final_retry_config,
            # both args
            broker_middlewares=middlewares,
            broker_parser=parser,
            broker_decoder=decoder,
            logger=make_gcp_logger_state(
                logger=logger
                if logger is not EMPTY and not isinstance(logger, object)
                else None,
                log_level=logging.INFO,
            )
            if logger is not EMPTY
            else make_gcp_logger_state(
                logger=logging.getLogger(__name__),
                log_level=logging.INFO,
            ),
            fd_config=FastDependsConfig(
                use_fastdepends=apply_types,
                serializer=serializer,
            ),
            # subscriber args
            broker_dependencies=dependencies,
            graceful_timeout=graceful_timeout,
            extra_context={
                "broker": self,
            },
        )

        if schema is None:
            schema = BrokerSpec(
                description=description,
                url=[specification_url or "https://pubsub.googleapis.com"],
                protocol=protocol or "gcp",
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
        reply_to: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """Publish message to GCP Pub/Sub topic.

        Args:
            message: Message body to send
            topic: GCP Pub/Sub topic name
            attributes: Message attributes for metadata
            ordering_key: Message ordering key
            reply_to: Reply topic for response messages (stored in attributes)
            correlation_id: Manual correlation ID setter

        Returns:
            Published message ID
        """
        # Add reply_to to attributes since GCP Pub/Sub doesn't have native reply_to
        final_attributes = attributes or {}
        if reply_to:
            final_attributes["reply_to"] = reply_to

        cmd = GCPPublishCommand(
            message,
            topic=topic or "",
            attributes=final_attributes,
            ordering_key=ordering_key,
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.PUBLISH,
        )

        result: str = await super()._basic_publish(
            cmd,
            producer=self.config.producer,
        )
        return result

    async def publish_batch(  # type: ignore[override]
        self,
        messages: list[Any],
        *,
        topic: str,
        attributes: dict[str, str] | None = None,
        ordering_key: str | None = None,
        correlation_id: str | None = None,
    ) -> list[str]:
        """Publish multiple messages to GCP Pub/Sub topic.

        Args:
            messages: List of message bodies to send
            topic: GCP Pub/Sub topic name
            attributes: Message attributes for metadata
            ordering_key: Message ordering key
            correlation_id: Base correlation ID for messages

        Returns:
            List of published message IDs
        """
        # Publish each message individually for now
        # TODO: Use true batch publishing when producer supports it
        message_ids = []
        for msg in messages:
            message_id = await self.publish(
                msg,
                topic=topic,
                attributes=attributes,
                ordering_key=ordering_key,
                correlation_id=correlation_id,
            )
            message_ids.append(message_id)

        return message_ids

    @override
    async def start(self) -> None:
        """Connect broker to GCP Pub/Sub and startup all subscribers."""
        await self.connect()
        await super().start()

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
