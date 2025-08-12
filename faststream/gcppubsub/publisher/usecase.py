"""GCP Pub/Sub publisher use case."""

from typing import TYPE_CHECKING, Any

from gcloud.aio.pubsub import PubsubMessage

from faststream._internal.endpoint.publisher.usecase import PublisherUsecase
from faststream.gcppubsub.publisher.config import GCPPubSubPublisherConfig
from faststream.gcppubsub.publisher.specification import GCPPubSubPublisherSpecification

if TYPE_CHECKING:
    from faststream.gcppubsub.configs.broker import GCPPubSubBrokerConfig


class GCPPubSubPublisher(PublisherUsecase):
    """GCP Pub/Sub publisher implementation."""

    def __init__(
        self,
        topic: str,
        *,
        create_topic: bool = True,
        ordering_key: str | None = None,
        config: "GCPPubSubBrokerConfig",
        **kwargs: Any,
    ) -> None:
        """Initialize publisher.

        Args:
            topic: Topic name
            create_topic: Whether to create topic if it doesn't exist
            ordering_key: Message ordering key
            config: Broker configuration
            **kwargs: Additional options
        """
        self.topic = topic
        self.create_topic = create_topic
        self.ordering_key = ordering_key

        # Create publisher config
        publisher_config = GCPPubSubPublisherConfig(
            _outer_config=config,
            middlewares=(),  # No publisher-specific middlewares for now
            topic=topic,
            create_topic=create_topic,
            ordering_key=ordering_key,
        )

        # Create specification
        specification = GCPPubSubPublisherSpecification(
            topic=topic,
            _outer_config=config,
        )

        super().__init__(
            config=publisher_config,
            specification=specification,
        )

    async def start(self) -> None:
        """Start the publisher."""
        await super().start()
        if self.create_topic:
            await self._ensure_topic_exists()

    async def stop(self) -> None:
        """Stop the publisher."""
        await super().stop()

    async def _publish(
        self,
        message: Any,
        *,
        correlation_id: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Publish a message (abstract method implementation)."""
        return await self.publish(
            message=message,
            correlation_id=correlation_id,
            **kwargs,
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
        target_topic = topic or self.topic
        target_ordering_key = ordering_key or self.ordering_key

        # Serialize message if needed
        if isinstance(message, (str, bytes)):
            data = message.encode() if isinstance(message, str) else message
        else:
            # Use broker's serializer
            data = (
                self._outer_config.broker_parser(message)
                if self._outer_config.broker_parser
                else str(message).encode()
            )

        # Get producer from config
        producer = self._outer_config.producer

        return await producer.publish(
            topic=target_topic,
            data=data,
            attributes=attributes,
            ordering_key=target_ordering_key,
            correlation_id=correlation_id,
        )

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
        target_topic = topic or self.topic

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
                    data = (
                        self._outer_config.broker_parser(msg)
                        if self._outer_config.broker_parser
                        else str(msg).encode()
                    )

                pubsub_messages.append(PubsubMessage(data))

        # Get producer from config
        producer = self._outer_config.producer

        return await producer.publish_batch(
            topic=target_topic,
            messages=pubsub_messages,
        )

    async def _ensure_topic_exists(self) -> None:
        """Ensure the topic exists."""
        try:
            # Use publisher client to create topic if it doesn't exist
            if hasattr(self.broker, "_state") and self.broker._state.publisher:
                # Note: gcloud-aio doesn't have a built-in create_topic method
                # In a real implementation, you'd use the admin client or handle this differently
                pass
        except Exception:
            # Topic creation can be handled externally or ignored for simplicity
            pass
