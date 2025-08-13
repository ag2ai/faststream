"""GCP Pub/Sub message publishing tests."""

import asyncio
from typing import Any

import pytest

from tests.brokers.base.publish import BrokerPublishTestcase
from tests.marks import require_gcp

from .basic import GCPTestcaseConfig


@pytest.mark.gcp()
@pytest.mark.connected()
@pytest.mark.flaky(reruns=3, reruns_delay=1)
@require_gcp
class TestPublish(GCPTestcaseConfig, BrokerPublishTestcase):
    """Test GCP Pub/Sub message publishing."""

    @pytest.mark.asyncio()
    async def test_publish_basic(self, subscription: str, topic: str) -> None:
        """Test basic message publishing."""
        pub_broker = self.get_broker()
        received_messages = []
        message_received = asyncio.Event()

        @pub_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            received_messages.append(msg)
            message_received.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            message_id = await br.publish("test message", topic=topic)
            await asyncio.wait(
                [asyncio.create_task(message_received.wait())], timeout=self.timeout
            )

        assert len(received_messages) == 1
        assert received_messages[0] == "test message"
        assert isinstance(message_id, str)

    @pytest.mark.asyncio()
    async def test_publish_with_attributes(self, subscription: str, topic: str) -> None:
        """Test publishing with message attributes."""
        pub_broker = self.get_broker()
        received_message = None
        message_received = asyncio.Event()

        @pub_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            nonlocal received_message
            received_message = msg
            message_received.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await br.publish(
                "test message",
                topic=topic,
                attributes={"source": "test", "priority": "high"},
            )
            await asyncio.wait(
                [asyncio.create_task(message_received.wait())], timeout=self.timeout
            )

        assert received_message is not None

    @pytest.mark.asyncio()
    async def test_publish_batch(self, subscription: str, topic: str) -> None:
        """Test batch message publishing."""
        pub_broker = self.get_broker()
        msgs_queue = asyncio.Queue(maxsize=3)

        @pub_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            await msgs_queue.put(msg)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            message_ids = await br.publish_batch(["msg1", "msg2", "msg3"], topic=topic)

            # Collect received messages
            received = []
            try:
                for _ in range(3):
                    msg = await asyncio.wait_for(msgs_queue.get(), timeout=2)
                    received.append(msg)
            except asyncio.TimeoutError:
                pass  # Got timeout, some messages may not have arrived

        assert len(received) == 3
        assert len(message_ids) == 3
        assert {"msg1", "msg2", "msg3"} == set(received)

    @pytest.mark.asyncio()
    async def test_publish_with_ordering_key(self, subscription: str, topic: str) -> None:
        """Test publishing with ordering keys."""
        pub_broker = self.get_broker()
        received_messages = []
        message_count = 0
        completion_event = asyncio.Event()

        @pub_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            nonlocal message_count
            received_messages.append(msg)
            message_count += 1
            if message_count >= 3:
                completion_event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            # Publish messages with same ordering key
            await br.publish("msg1", topic=topic, ordering_key="key1")
            await br.publish("msg2", topic=topic, ordering_key="key1")
            await br.publish("msg3", topic=topic, ordering_key="key2")

            await asyncio.wait(
                [asyncio.create_task(completion_event.wait())], timeout=self.timeout
            )

        assert len(received_messages) == 3

    @pytest.mark.asyncio()
    async def test_publish_large_message(self, subscription: str, topic: str) -> None:
        """Test publishing large messages."""
        pub_broker = self.get_broker()
        received_message = None
        message_received = asyncio.Event()

        @pub_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            nonlocal received_message
            received_message = msg
            message_received.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            # Create large message (but within GCP limits)
            large_message = "x" * 10000  # 10KB message
            await br.publish(large_message, topic=topic)
            await asyncio.wait(
                [asyncio.create_task(message_received.wait())], timeout=self.timeout
            )

        assert received_message == large_message

    @pytest.mark.asyncio()
    async def test_publish_json_serialization(
        self, subscription: str, topic: str
    ) -> None:
        """Test publishing with automatic JSON serialization."""
        pub_broker = self.get_broker()
        received_message = None
        message_received = asyncio.Event()

        @pub_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            nonlocal received_message
            received_message = msg
            message_received.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            complex_data = {
                "name": "test",
                "values": [1, 2, 3],
                "nested": {"key": "value"},
            }
            await br.publish(complex_data, topic=topic)
            await asyncio.wait(
                [asyncio.create_task(message_received.wait())], timeout=self.timeout
            )

        # The exact format depends on serialization implementation
        assert received_message is not None

    @pytest.mark.asyncio()
    async def test_publish_with_correlation_id(
        self, subscription: str, topic: str
    ) -> None:
        """Test publishing with correlation ID."""
        pub_broker = self.get_broker()
        received_msg = None
        message_received = asyncio.Event()

        @pub_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            nonlocal received_msg
            received_msg = msg
            message_received.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            correlation_id = "test-correlation-123"
            await br.publish("test message", topic=topic, correlation_id=correlation_id)
            await asyncio.wait(
                [asyncio.create_task(message_received.wait())], timeout=self.timeout
            )

        assert received_msg is not None

    @pytest.mark.asyncio()
    async def test_publish_return_message_id(self, subscription: str, topic: str) -> None:
        """Test that publish returns a valid message ID."""
        pub_broker = self.get_broker()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            message_id = await br.publish("test message", topic=topic)

        assert isinstance(message_id, str)
        assert len(message_id) > 0

    @pytest.mark.asyncio()
    async def test_publish_bytes_message(self, subscription: str, topic: str) -> None:
        """Test publishing binary data."""
        pub_broker = self.get_broker()
        received_message = None
        message_received = asyncio.Event()

        @pub_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            nonlocal received_message
            received_message = msg
            message_received.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            binary_data = b"binary test data"
            await br.publish(binary_data, topic=topic)
            await asyncio.wait(
                [asyncio.create_task(message_received.wait())], timeout=self.timeout
            )

        # Note: The exact format may depend on how binary data is handled
        assert received_message is not None

    @pytest.mark.asyncio()
    async def test_publish_multiple_topics(self) -> None:
        """Test publishing to multiple topics."""
        pub_broker = self.get_broker()

        # Generate unique topic names
        topic1 = "test-topic-1"
        topic2 = "test-topic-2"

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            # Publish to different topics
            msg_id1 = await br.publish("message1", topic=topic1)
            msg_id2 = await br.publish("message2", topic=topic2)

        assert isinstance(msg_id1, str)
        assert isinstance(msg_id2, str)
        assert msg_id1 != msg_id2  # Should be different message IDs

    @pytest.mark.asyncio()
    async def test_publish_empty_message(self, subscription: str, topic: str) -> None:
        """Test publishing empty message."""
        pub_broker = self.get_broker()
        received_message = None
        message_received = asyncio.Event()

        @pub_broker.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            nonlocal received_message
            received_message = msg
            message_received.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await br.publish("", topic=topic)  # Empty string
            await asyncio.wait(
                [asyncio.create_task(message_received.wait())], timeout=self.timeout
            )

        assert received_message == ""
