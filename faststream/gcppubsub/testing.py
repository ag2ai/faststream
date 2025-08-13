"""GCP Pub/Sub testing utilities."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

from gcloud.aio.pubsub import PubsubMessage

from faststream._internal.testing.broker import TestBroker, change_producer
from faststream._internal.testing.serialization import (
    serialize_with_broker_serializer,
    serialize_with_json,
)
from faststream.exceptions import SubscriberNotFound
from faststream.gcppubsub.broker import GCPPubSubBroker
from faststream.gcppubsub.response import GCPPubSubPublishCommand
from faststream.message import gen_cor_id

if TYPE_CHECKING:
    from faststream.gcppubsub.publisher.usecase import GCPPubSubPublisher
    from faststream.gcppubsub.subscriber.usecase import GCPPubSubSubscriber

__all__ = ("TestGCPPubSubBroker",)


class TestGCPPubSubBroker(TestBroker[GCPPubSubBroker]):
    """A class to test GCP Pub/Sub brokers."""

    @contextmanager
    def _patch_producer(self, broker: GCPPubSubBroker) -> Iterator[None]:
        fake_producer = FakeGCPPubSubProducer(broker)
        with change_producer(broker.config.broker_config, fake_producer):
            yield

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: GCPPubSubBroker,
        publisher: "GCPPubSubPublisher",
    ) -> tuple["GCPPubSubSubscriber", bool]:
        """Create a fake subscriber for publisher testing."""
        sub: GCPPubSubSubscriber | None = None

        # Look for existing subscriber that matches the publisher's topic
        for handler in broker.subscribers:
            handler = cast("GCPPubSubSubscriber", handler)
            # Check if subscriber matches publisher topic
            # If subscriber topic is None, it defaults to subscription name
            handler_topic = (
                handler.topic if handler.topic is not None else handler.subscription
            )
            if handler_topic == publisher.topic:
                sub = handler
                break

        if sub is None:
            # Create a fake subscriber if none exists
            is_real = False
            sub = broker.subscriber(
                subscription=f"fake-sub-{publisher.topic}",
                topic=publisher.topic,
                create_subscription=True,  # Allow creation for real connections
            )
        else:
            is_real = True

        return sub, is_real

    @staticmethod
    async def _fake_connect(broker: GCPPubSubBroker, *args: Any, **kwargs: Any) -> None:
        """Fake connection method."""


class FakeGCPPubSubProducer:
    """A fake GCP Pub/Sub producer for testing purposes."""

    def __init__(self, broker: GCPPubSubBroker) -> None:
        self.broker = broker

    def _serialize_message_data(self, message_data: Any, attrs: dict[str, Any]) -> bytes:
        """Serialize message data based on its type."""
        if isinstance(message_data, str):
            attrs["content_type"] = "text/plain"
            return message_data.encode()
        if isinstance(message_data, bytes):
            # Keep as-is, no content type
            return message_data
        # For other types, use serialization
        attrs["content_type"] = "application/json"
        serializer = self._get_broker_serializer()

        if serializer:
            return serialize_with_broker_serializer(message_data, serializer)
        return serialize_with_json(message_data)

    def _get_broker_serializer(self) -> Any | None:
        """Get the broker's serializer if available."""
        serializer = getattr(self.broker.config, "fd_config", None)
        if serializer and hasattr(serializer, "_serializer"):
            return serializer._serializer
        return None

    async def _execute_matching_handlers(self, message: Any, topic: str) -> None:
        """Execute handlers that match the topic."""
        from typing import cast

        for handler in self.broker.subscribers:
            handler = cast("GCPPubSubSubscriber", handler)
            if _is_handler_matches(handler, topic):
                await self._execute_handler(message, handler)

    async def publish(
        self,
        cmd: GCPPubSubPublishCommand,
    ) -> str:
        """Publish a message to a topic (fake implementation)."""
        # Extract data from command
        message_data = cmd.message
        attrs = cmd.attributes or {}
        ordering_key = cmd.ordering_key
        correlation_id = cmd.correlation_id or gen_cor_id()
        topic = cmd.topic

        # Ensure correlation_id in attributes
        attrs["correlation_id"] = correlation_id

        # Serialize the message data
        data = self._serialize_message_data(message_data, attrs)

        # Build the message
        message = build_message(
            data=data,
            attributes=attrs,
            ordering_key=ordering_key,
            correlation_id=correlation_id,
        )

        # Find matching subscribers and execute handlers
        await self._execute_matching_handlers(message, topic)

        return str(message.attributes.get("message_id", "test-message-id"))

    async def publish_batch(
        self,
        cmd: Any,
    ) -> list[str]:
        """Publish multiple messages to a topic (fake implementation)."""
        message_ids = []

        # Handle different batch formats
        if isinstance(cmd, list):
            # List of GCPPubSubPublishCommand objects
            for command in cmd:
                msg_id = await self.publish(command)
                message_ids.append(msg_id)
        elif hasattr(cmd, "messages") and cmd.messages:
            # Single command with messages array
            for msg in cmd.messages:
                # Create proper GCPPubSubPublishCommand
                proper_cmd = GCPPubSubPublishCommand(
                    message=msg,
                    topic=cmd.topic,
                    attributes=getattr(cmd, "attributes", {}),
                    ordering_key=getattr(cmd, "ordering_key", None),
                    correlation_id=gen_cor_id(),
                )
                msg_id = await self.publish(proper_cmd)
                message_ids.append(msg_id)

        return message_ids

    async def request(
        self,
        cmd: Any,
    ) -> Any:
        """Send a request and wait for response (fake implementation)."""
        # Extract data from command
        message_data = cmd.message
        topic = cmd.topic
        attrs = cmd.attributes or {}
        correlation_id = cmd.correlation_id or gen_cor_id()

        # Convert message to bytes if needed
        if isinstance(message_data, str):
            data = message_data.encode()
        elif isinstance(message_data, bytes):
            data = message_data
        else:
            data = str(message_data).encode()

        message = build_message(
            data=data,
            attributes=attrs,
            correlation_id=correlation_id,
        )

        # Find matching subscribers and execute handler
        for handler in self.broker.subscribers:
            handler = cast("GCPPubSubSubscriber", handler)
            if _is_handler_matches(handler, topic):
                return await self._execute_handler(message, handler)

        raise SubscriberNotFound

    async def _execute_handler(
        self,
        msg: PubsubMessage,
        handler: "GCPPubSubSubscriber",
    ) -> Any:
        """Execute a message handler."""
        # Ensure handler is running (for test mode)
        if not handler.running:
            handler.running = True

        # Pass the raw PubsubMessage directly - process_message expects it
        # The parser will wrap it in GCPPubSubMessage
        return await handler.process_message(msg)


def build_message(
    data: bytes,
    *,
    attributes: dict[str, str] | None = None,
    ordering_key: str | None = None,
    correlation_id: str | None = None,
    message_id: str | None = None,
) -> PubsubMessage:
    """Build a test GCP Pub/Sub message."""
    attrs = attributes or {}
    if correlation_id:
        attrs["correlation_id"] = correlation_id

    # Add message_id to attributes since that's where PubsubMessage stores it
    msg_id = message_id or f"test-msg-{gen_cor_id()}"
    attrs["message_id"] = msg_id

    # Create message - everything except data and ordering_key goes in attributes
    return PubsubMessage(data=data, ordering_key=ordering_key or "", **attrs)


def _is_handler_matches(
    handler: "GCPPubSubSubscriber",
    topic: str,
) -> bool:
    """Check if a handler matches a topic."""
    return hasattr(handler, "topic") and handler.topic == topic


# Convenience function for creating test messages
def create_test_message(
    data: str | bytes,
    *,
    attributes: dict[str, str] | None = None,
    message_id: str | None = None,
    ordering_key: str | None = None,
) -> PubsubMessage:
    """Create a test PubSub message.

    Args:
        data: Message data
        attributes: Message attributes
        message_id: Message ID
        ordering_key: Message ordering key

    Returns:
        PubsubMessage instance
    """
    if isinstance(data, str):
        data = data.encode()

    return build_message(
        data=data,
        attributes=attributes,
        message_id=message_id,
        ordering_key=ordering_key,
    )
