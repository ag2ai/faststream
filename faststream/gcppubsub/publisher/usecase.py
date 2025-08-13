"""GCP Pub/Sub publisher use case."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from gcloud.aio.pubsub import PubsubMessage

from faststream._internal.endpoint.publisher.usecase import PublisherUsecase
from faststream.gcppubsub.publisher.config import GCPPubSubPublisherConfig
from faststream.gcppubsub.publisher.specification import GCPPubSubPublisherSpecification

if TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware
    from faststream.gcppubsub.configs.broker import GCPPubSubBrokerConfig
    from faststream.response.response import PublishCommand


class GCPPubSubPublisher(PublisherUsecase):
    """GCP Pub/Sub publisher implementation."""

    def __init__(
        self,
        topic: str,
        *,
        create_topic: bool = True,
        ordering_key: str | None = None,
        middlewares: Sequence["PublisherMiddleware"] = (),
        config: "GCPPubSubBrokerConfig",
        title_: str | None = None,
        description_: str | None = None,
        include_in_schema: bool = True,
        **kwargs: Any,
    ) -> None:
        """Initialize publisher.

        Args:
            topic: Topic name
            create_topic: Whether to create topic if it doesn't exist
            ordering_key: Message ordering key
            middlewares: Publisher middlewares
            config: Broker configuration
            title_: AsyncAPI title
            description_: AsyncAPI description
            include_in_schema: Whether to include in schema
            **kwargs: Additional options
        """
        self.topic = topic
        self.create_topic = create_topic
        self.ordering_key = ordering_key

        # Create publisher config
        publisher_config = GCPPubSubPublisherConfig(
            _outer_config=config,
            middlewares=middlewares,
            topic=topic,
            create_topic=create_topic,
            ordering_key=ordering_key,
        )

        # Create specification
        specification = GCPPubSubPublisherSpecification(
            topic=topic,
            _outer_config=config,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

        super().__init__(
            config=publisher_config,
            specification=specification,
        )

    def get_topic_name(self) -> str:
        """Get topic name with prefix applied."""
        return f"{self._outer_config.prefix}{self.topic}"

    async def start(self) -> None:
        """Start the publisher."""
        await super().start()
        if self.create_topic:
            await self._ensure_topic_exists()

    async def stop(self) -> None:
        """Stop the publisher."""
        # No cleanup needed for GCP Pub/Sub publisher

    async def _publish(
        self,
        cmd: "PublishCommand",
        *,
        _extra_middlewares: Any = None,
    ) -> None:
        """Publish a message (abstract method implementation)."""
        from faststream.gcppubsub.response import GCPPubSubPublishCommand

        # Convert generic PublishCommand to GCPPubSubPublishCommand if needed
        if isinstance(cmd, GCPPubSubPublishCommand):
            gcp_cmd = cmd
        else:
            # Use cmd.destination if it's truthy, otherwise fall back to prefixed topic
            destination = getattr(cmd, "destination", None) or self.get_topic_name()
            gcp_cmd = GCPPubSubPublishCommand(
                message=cmd.body,
                topic=destination,
                attributes=cmd.headers if isinstance(cmd.headers, dict) else {},
                correlation_id=cmd.correlation_id,
                _publish_type=cmd.publish_type,
            )

        # Use _basic_publish to properly handle publisher middleware
        await self._basic_publish(
            gcp_cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares or (),
        )

    async def request(
        self,
        message: Any,
        *,
        correlation_id: str | None = None,
        timeout: float = 30.0,
        **kwargs: Any,
    ) -> Any:
        """Send a request and wait for response."""
        # GCP Pub/Sub doesn't natively support request-reply
        # This could be implemented using correlation IDs and response topics
        msg = (
            "Request-reply pattern is not natively supported in GCP Pub/Sub. "
            "Consider implementing using correlation IDs and a response subscription."
        )
        raise NotImplementedError(msg)

    async def publish(
        self,
        message: Any,
        *,
        topic: str | None = None,
        attributes: dict[str, str] | None = None,
        ordering_key: str | None = None,
        correlation_id: str | None = None,
        **kwargs: Any,
    ) -> str:
        """Publish a message.

        Args:
            message: Message to publish
            topic: Override topic name
            attributes: Message attributes
            ordering_key: Override ordering key
            correlation_id: Message correlation ID
            **kwargs: Additional options

        Returns:
            Published message ID
        """
        target_topic = topic or self.get_topic_name()
        target_ordering_key = ordering_key or self.ordering_key

        # Use FastStream's encoding logic for consistency
        from faststream.message import encode_message

        try:
            data, content_type = encode_message(message, serializer=None)

            # Handle empty messages like the broker does
            if not data:
                data = b" "
                attributes = attributes or {}
                attributes["__faststream_empty"] = "true"
        except TypeError as e:
            if "not JSON serializable" in str(e):
                # Fallback for non-serializable objects
                import json
                from dataclasses import asdict, is_dataclass
                from datetime import datetime

                def json_serializer(obj: Any) -> Any:
                    if isinstance(obj, datetime):
                        return obj.isoformat()
                    if is_dataclass(obj) and not isinstance(obj, type):
                        return asdict(obj)
                    if hasattr(obj, "model_dump"):
                        return obj.model_dump()
                    if hasattr(obj, "dict"):
                        return obj.dict()
                    error_msg = f"Object of type {obj.__class__.__name__} is not JSON serializable"
                    raise TypeError(error_msg)

                data = json.dumps(message, default=json_serializer).encode()
                content_type = "application/json"
            else:
                raise

        # Get producer from config
        producer = self._outer_config.producer

        # Create GCP Pub/Sub command object
        from faststream.gcppubsub.response import GCPPubSubPublishCommand

        # Merge content_type into attributes
        final_attributes = attributes or {}
        if "content_type" in locals() and content_type:
            final_attributes["content_type"] = content_type

        cmd = GCPPubSubPublishCommand(
            message=data,
            topic=target_topic,
            attributes=final_attributes,
            ordering_key=target_ordering_key,
            correlation_id=correlation_id,
        )

        result = await producer.publish(cmd)
        return result if isinstance(result, str) else ""

    async def publish_batch(
        self,
        messages: list[Any],
        *,
        topic: str | None = None,
        **kwargs: Any,
    ) -> list[str]:
        """Publish multiple messages.

        Args:
            messages: Messages to publish
            topic: Override topic name
            **kwargs: Additional options

        Returns:
            List of published message IDs
        """
        target_topic = topic or self.get_topic_name()

        # Convert messages to PubsubMessage objects
        pubsub_messages = []
        for msg in messages:
            if isinstance(msg, PubsubMessage):
                pubsub_messages.append(msg)
            else:
                # Serialize message
                if isinstance(msg, (str, bytes)):
                    data = msg.encode() if isinstance(msg, str) else msg
                else:
                    # Simple serialization - convert to JSON if needed
                    import json

                    try:
                        data = json.dumps(msg).encode()
                    except (TypeError, ValueError):
                        data = str(msg).encode()

                pubsub_messages.append(PubsubMessage(data))

        # Get producer from config
        producer = self._outer_config.producer

        # Create GCP Pub/Sub command objects
        from faststream.gcppubsub.response import GCPPubSubPublishCommand

        commands = [
            GCPPubSubPublishCommand(
                message=msg.data,
                topic=target_topic,
                attributes=msg.attributes or {},
            )
            for msg in pubsub_messages
        ]

        result = await producer.publish_batch(commands)
        return result if isinstance(result, list) else []

    async def _ensure_topic_exists(self) -> None:
        """Ensure the topic exists."""
        try:
            # Use publisher client to create topic if it doesn't exist
            if (
                hasattr(self._outer_config, "connection")
                and self._outer_config.connection
            ):
                publisher_client = self._outer_config.connection.publisher
                # Create full topic path
                # Type cast needed since base class expects BrokerConfig but we have GCPPubSubBrokerConfig
                config = cast("GCPPubSubBrokerConfig", self._outer_config)
                topic_path = (
                    f"projects/{config.project_id}/topics/{self.get_topic_name()}"
                )
                await publisher_client.create_topic(topic_path)
        except Exception:  # nosec B110
            # Topic might already exist or creation failed - continue anyway
            pass
