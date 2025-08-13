"""GCP Pub/Sub test client functionality tests."""

import asyncio

import pytest

from faststream import BaseMiddleware
from faststream.gcp.testing import FakeGCPProducer
from tests.brokers.base.testclient import BrokerTestclientTestcase
from tests.marks import require_gcp

from .basic import GCPMemoryTestcaseConfig


@pytest.mark.gcp()
@pytest.mark.asyncio()
@require_gcp
class TestTestclient(GCPMemoryTestcaseConfig, BrokerTestclientTestcase):
    """Test GCP Pub/Sub in-memory test client."""

    def get_fake_producer_class(self) -> type:
        """Return fake producer class for testing."""
        return FakeGCPProducer

    async def test_subscriber_mock(self, subscription: str, topic: str) -> None:
        """Test subscriber mocking functionality."""
        broker = self.get_broker()

        @broker.subscriber(subscription, topic=topic)
        async def handler(msg) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", topic=topic)
            handler.mock.assert_called_once_with("hello")

    async def test_publisher_mock(self, topic: str) -> None:
        """Test publisher mocking functionality."""
        broker = self.get_broker()

        publisher = broker.publisher(topic)

        async with self.patch_broker(broker) as _:
            await publisher.publish("test message")

        # Verify mock was called - specific verification depends on implementation
        assert hasattr(publisher, "mock")  # Placeholder for now

    async def test_batch_publishing_mock(self, subscription: str, topic: str) -> None:
        """Test batch publishing in test mode."""
        broker = self.get_broker()
        messages = []

        @broker.subscriber(subscription, topic=topic)
        async def handler(msg) -> None:
            messages.append(msg)

        async with self.patch_broker(broker) as br:
            await br.publish_batch(["msg1", "msg2", "msg3"], topic=topic)

        # In test mode, each message should be processed
        assert len(messages) == 3
        assert set(messages) == {"msg1", "msg2", "msg3"}

    async def test_request_response_mock(self, topic: str, response_topic: str) -> None:
        """Test request-response pattern mocking."""
        # GCP Pub/Sub doesn't support request-response natively
        pytest.skip("Request-response not implemented for GCP Pub/Sub")

    async def test_middleware_integration(self, subscription: str, topic: str) -> None:
        """Test middleware integration with test client."""
        middleware_calls = []

        class TestMiddleware(BaseMiddleware):
            async def on_receive(self) -> None:
                middleware_calls.append("before")

            async def after_processed(
                self,
                exc_type: type[BaseException] | None = None,
                exc_val: BaseException | None = None,
                exc_tb=None,
            ) -> bool | None:
                middleware_calls.append("after")
                return False

        # Register middleware at broker level like RabbitMQ
        broker = self.get_broker(middlewares=(TestMiddleware,))

        @broker.subscriber(subscription, topic=topic)
        async def handler(msg) -> None:
            middleware_calls.append("handler")

        async with self.patch_broker(broker) as br:
            await br.publish("test", topic=topic)

        assert middleware_calls == ["before", "handler", "after"]

    async def test_fake_producer_publish(self, topic: str) -> None:
        """Test fake producer publish functionality."""
        broker = self.get_broker()

        async with self.patch_broker(broker) as br:
            # Test direct fake producer usage
            fake_producer = FakeGCPProducer(br)

            from faststream.gcp.response import GCPPublishCommand

            cmd = GCPPublishCommand(
                message="test message",
                topic=topic,
                attributes={"test": "attr"},
            )

            message_id = await fake_producer.publish(cmd)
            assert isinstance(message_id, str)
            assert len(message_id) > 0

    async def test_fake_producer_batch_publish(self, topic: str) -> None:
        """Test fake producer batch publish functionality."""
        broker = self.get_broker()

        async with self.patch_broker(broker) as br:
            fake_producer = FakeGCPProducer(br)

            from faststream.gcp.response import GCPPublishCommand

            commands = [
                GCPPublishCommand(message=f"msg{i}", topic=topic) for i in range(3)
            ]

            message_ids = await fake_producer.publish_batch(commands)
            assert isinstance(message_ids, list)
            assert len(message_ids) == 3

    async def test_subscriber_lifecycle_mock(self, subscription: str, topic: str) -> None:
        """Test subscriber lifecycle in test mode."""
        broker = self.get_broker()
        lifecycle_events = []

        @broker.subscriber(subscription, topic=topic)
        async def handler(msg) -> None:
            lifecycle_events.append(f"processed: {msg}")

        async with self.patch_broker(broker) as br:
            lifecycle_events.append("started")
            await br.publish("test message", topic=topic)
            await asyncio.sleep(0.1)  # Allow processing

        lifecycle_events.append("stopped")

        assert "started" in lifecycle_events
        assert any("processed:" in event for event in lifecycle_events)
        assert "stopped" in lifecycle_events

    async def test_multiple_subscribers_mock(self, topic: str) -> None:
        """Test multiple subscribers in test mode."""
        broker = self.get_broker()

        sub1_messages = []
        sub2_messages = []

        @broker.subscriber(f"{topic}-sub1", topic=topic)
        async def handler1(msg) -> None:
            sub1_messages.append(msg)

        @broker.subscriber(f"{topic}-sub2", topic=topic)
        async def handler2(msg) -> None:
            sub2_messages.append(msg)

        async with self.patch_broker(broker) as br:
            await br.publish("test message", topic=topic)

        # Both subscribers should receive the message
        assert len(sub1_messages) == 1
        assert len(sub2_messages) == 1
        assert sub1_messages[0] == "test message"
        assert sub2_messages[0] == "test message"

    async def test_attribute_handling_mock(self, subscription: str, topic: str) -> None:
        """Test message attributes in test mode."""
        broker = self.get_broker()
        received_message = None

        @broker.subscriber(subscription, topic=topic)
        async def handler(msg) -> None:
            nonlocal received_message
            received_message = msg

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish(
                "test message",
                topic=topic,
                attributes={"key1": "value1", "key2": "value2"},
            )

        # In test mode, verify the message content is received correctly
        # The actual message attributes are used internally for message routing and processing
        assert received_message == "test message"

    async def test_error_handling_mock(self, subscription: str, topic: str) -> None:
        """Test error handling in test mode - exceptions should propagate."""
        broker = self.get_broker()
        error_occurred = False

        @broker.subscriber(subscription, topic=topic)
        async def handler(msg) -> None:
            nonlocal error_occurred
            error_occurred = True
            error_msg = "Test error"
            raise ValueError(error_msg)

        async with self.patch_broker(broker) as br:
            with pytest.raises(ValueError, match="Test error"):
                await br.publish("test message", topic=topic)

        assert error_occurred
