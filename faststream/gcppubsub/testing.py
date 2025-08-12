"""GCP Pub/Sub testing utilities."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

from gcloud.aio.pubsub import PubsubMessage

from faststream._internal.testing.broker import TestBroker, change_producer
from faststream.exceptions import SubscriberNotFound
from faststream.gcppubsub.broker import GCPPubSubBroker
from faststream.gcppubsub.message import GCPPubSubMessage
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
            if hasattr(handler, "topic") and handler.topic == publisher.topic:
                sub = handler
                break

        if sub is None:
            # Create a fake subscriber if none exists
            is_real = False
            sub = broker.subscriber(
                subscription=f"fake-sub-{publisher.topic}",
                topic=publisher.topic,
                create_subscription=False,
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

        # Ensure correlation_id in attributes
        attrs["correlation_id"] = correlation_id

        # Convert message to bytes if needed
        if isinstance(message_data, str):
            data = message_data.encode()
        elif isinstance(message_data, bytes):
            data = message_data
        else:
            # Serialize other types to string then bytes
            data = str(message_data).encode()

        # Build the message
        message = build_message(
            data=data,
            attributes=attrs,
            ordering_key=ordering_key,
            correlation_id=correlation_id,
        )

        # For now, just return the message ID without executing handlers
        # The FastStream testing framework handles handler mocking differently
        return str(message.attributes.get("message_id", "test-message-id"))

    async def publish_batch(
        self,
        cmd: Any,
    ) -> list[str]:
        """Publish multiple messages to a topic (fake implementation)."""
        # For now, just call publish for each message in batch
        # This is a simplified implementation
        message_ids = []
        if hasattr(cmd, "messages") and cmd.messages:
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
    ) -> GCPPubSubMessage:
        """Execute a message handler."""
        # Get message_id from attributes
        message_id = msg.attributes.get("message_id", "unknown")

        # Wrap in FastStream message
        fs_message = GCPPubSubMessage(
            raw_message=msg,
            ack_id=f"ack-{message_id}",
            subscription=handler.subscription,
        )

        # Process message through handler
        result = await handler.process_message(fs_message)

        # Return processed message as GCPPubSubMessage
        pubsub_msg = build_message(
            data=str(result.body).encode() if result.body else b"",
            attributes=result.headers or {},
            correlation_id=result.correlation_id or "",
        )

        return GCPPubSubMessage(
            raw_message=pubsub_msg,
            ack_id=f"test-ack-{gen_cor_id()}",
            subscription=handler.subscription,
        )


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
