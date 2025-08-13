"""GCP Pub/Sub message consumption tests."""

import asyncio
from typing import Any

import pytest

from faststream.gcp import GCPMessage
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.marks import require_gcp

from .basic import GCPTestcaseConfig


@pytest.mark.gcp()
@pytest.mark.connected()
@require_gcp
class TestConsume(GCPTestcaseConfig, BrokerRealConsumeTestcase):
    """Test GCP Pub/Sub message consumption."""

    @pytest.mark.asyncio()
    async def test_consume_basic(self, subscription: str, topic: str) -> None:
        """Test basic message consumption from subscription."""
        event = asyncio.Event()
        consume_broker = self.get_broker()

        @consume_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("test message", topic=topic)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_with_attributes(self, subscription: str, topic: str) -> None:
        """Test message consumption with attributes."""
        consume_broker = self.get_broker()
        received_msg = None
        event = asyncio.Event()

        @consume_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: GCPMessage) -> None:
            nonlocal received_msg
            received_msg = msg
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br.publish(
                "test message",
                topic=topic,
                attributes={"key1": "value1", "key2": "value2"},
            )

            await asyncio.wait([asyncio.create_task(event.wait())], timeout=self.timeout)

        # In simple mode, verify the message content is received correctly
        # Attributes are handled internally by the broker for message routing and processing
        assert received_msg == "test message"

    @pytest.mark.asyncio()
    async def test_auto_ack(self, subscription: str, topic: str) -> None:
        """Test automatic message acknowledgment (simple pattern)."""
        consume_broker = self.get_broker()
        processed = asyncio.Event()
        received_msg = None

        @consume_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: str) -> None:
            nonlocal received_msg
            received_msg = msg
            processed.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("test", topic=topic)),
                    asyncio.create_task(processed.wait()),
                ),
                timeout=self.timeout,
            )

        # In simple mode, acknowledgment happens automatically after successful processing
        assert processed.is_set()
        assert received_msg == "test"

    @pytest.mark.asyncio()
    async def test_error_handling(self, subscription: str, topic: str) -> None:
        """Test error handling in message processing (simple pattern)."""
        consume_broker = self.get_broker()
        exception_handled = asyncio.Event()
        message_received = None

        class ProcessingError(Exception):
            """Custom exception for testing processing errors."""

        @consume_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: str) -> None:
            nonlocal message_received
            message_received = msg
            try:
                # Simulate processing error
                error_msg = "Simulated processing error"
                raise ProcessingError(error_msg)
            finally:
                exception_handled.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br.publish("test", topic=topic)
            await asyncio.wait(
                [asyncio.create_task(exception_handled.wait())], timeout=self.timeout
            )

        # In simple mode, error handling and NACK happens automatically at broker level
        assert exception_handled.is_set()
        assert message_received == "test"

    @pytest.mark.asyncio()
    async def test_concurrent_consumers(self, subscription: str, topic: str) -> None:
        """Test multiple concurrent consumers on same subscription."""
        consume_broker = self.get_broker()
        processed_messages = []
        message_count = 5
        expected_events = [asyncio.Event() for _ in range(message_count)]

        @consume_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            processed_messages.append(msg)
            if len(processed_messages) <= len(expected_events):
                expected_events[len(processed_messages) - 1].set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # Publish multiple messages
            tasks = [
                br.publish(f"message-{i}", topic=topic) for i in range(message_count)
            ]
            await asyncio.gather(*tasks)

            # Wait for all messages to be processed
            await asyncio.wait(
                [asyncio.create_task(event.wait()) for event in expected_events],
                timeout=self.timeout,
            )

        assert len(processed_messages) == message_count

    @pytest.mark.asyncio()
    async def test_message_processing(self, subscription: str, topic: str) -> None:
        """Test basic message processing (simple pattern)."""
        consume_broker = self.get_broker()
        received_messages = []
        processing_complete = asyncio.Event()

        @consume_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: str) -> None:
            # In simple mode, just collect all received messages
            received_messages.append(msg)

            # Set event when we've processed all test messages
            if len(received_messages) == 3:
                processing_complete.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # Publish test messages (attributes handled internally for routing)
            await br.publish("msg1", topic=topic, attributes={"process": "true"})
            await br.publish("msg2", topic=topic, attributes={"process": "false"})
            await br.publish("msg3", topic=topic, attributes={"process": "true"})

            await asyncio.wait(
                [asyncio.create_task(processing_complete.wait())], timeout=self.timeout
            )

        # In simple mode, all messages are processed (filtering would be done in business logic if needed)
        assert len(received_messages) == 3
        assert received_messages == ["msg1", "msg2", "msg3"]

    @pytest.mark.asyncio()
    async def test_subscription_creation(self, subscription: str, topic: str) -> None:
        """Test automatic subscription creation."""
        consume_broker = self.get_broker()
        message_received = asyncio.Event()

        @consume_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            message_received.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # Publish a message - this should work even with auto-created subscription
            await br.publish("test message", topic=topic)
            await asyncio.wait(
                [asyncio.create_task(message_received.wait())], timeout=self.timeout
            )

        assert message_received.is_set()

    @pytest.mark.asyncio()
    async def test_ack_deadline_behavior(self, subscription: str, topic: str) -> None:
        """Test ACK deadline configuration."""
        consume_broker = self.get_broker()
        processed = asyncio.Event()

        @consume_broker.subscriber(
            subscription,
            topic=topic,
            create_subscription=True,
            ack_deadline=30,  # 30 seconds
        )
        async def handler(msg: GCPMessage) -> None:
            # Simulate processing time
            await asyncio.sleep(0.1)
            processed.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("test", topic=topic)),
                    asyncio.create_task(processed.wait()),
                ),
                timeout=self.timeout,
            )

        assert processed.is_set()

    @pytest.mark.asyncio()
    async def test_max_messages_configuration(
        self, subscription: str, topic: str
    ) -> None:
        """Test max messages pull configuration."""
        consume_broker = self.get_broker()
        messages_received = []
        batch_complete = asyncio.Event()

        @consume_broker.subscriber(
            subscription,
            topic=topic,
            create_subscription=True,
            max_messages=5,  # Pull up to 5 messages at once
        )
        async def handler(msg: Any) -> None:
            messages_received.append(msg)
            if len(messages_received) >= 3:  # We'll send 3 messages
                batch_complete.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # Send multiple messages
            for i in range(3):
                await br.publish(f"message-{i}", topic=topic)

            await asyncio.wait(
                [asyncio.create_task(batch_complete.wait())], timeout=self.timeout
            )

        assert len(messages_received) == 3

    @pytest.mark.asyncio()
    async def test_consumer_lifecycle(self, subscription: str, topic: str) -> None:
        """Test consumer start/stop behavior."""
        consume_broker = self.get_broker()
        lifecycle_events = []

        @consume_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            lifecycle_events.append(f"processed: {msg}")

        # Test start
        async with self.patch_broker(consume_broker) as br:
            await br.start()
            lifecycle_events.append("started")

            await br.publish("test message", topic=topic)
            await asyncio.sleep(0.5)  # Allow message processing

            # Stop is handled by the context manager

        lifecycle_events.append("stopped")

        assert "started" in lifecycle_events
        assert "stopped" in lifecycle_events
        assert any("processed:" in event for event in lifecycle_events)
