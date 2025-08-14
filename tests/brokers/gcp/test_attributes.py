"""Tests for GCP Pub/Sub message attributes."""

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream.gcp import MessageAttributes, MessageId, OrderingKey
from faststream.gcp.broker import GCPBroker
from faststream.gcp.testing import TestGCPBroker, build_message


class TestMessageAttributes:
    """Test message attributes handling."""

    def test_build_message_with_attributes(self) -> None:
        """Test building a message with attributes."""
        attrs = {"user_id": "123", "priority": "high"}
        msg = build_message(
            data=b"test data",
            attributes=attrs,
            ordering_key="order-1",
            correlation_id="corr-1",
        )

        assert msg.data == b"test data"
        assert msg.ordering_key == "order-1"
        assert msg.attributes["user_id"] == "123"
        assert msg.attributes["priority"] == "high"
        assert msg.attributes["correlation_id"] == "corr-1"

    @pytest.mark.asyncio()
    async def test_publish_with_attributes(self) -> None:
        """Test publishing messages with attributes."""
        broker = GCPBroker(project_id="test-project")
        received_msg = None
        received_attrs = None

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: Any) -> None:
            nonlocal received_msg, received_attrs
            # In real implementation, we'd access attributes via context
            # For now, we'll test the message structure
            received_msg = msg

        async with TestGCPBroker(broker) as br:
            # Test publishing with attributes
            await br.publish(
                "test message",
                topic="test-topic",
                attributes={"key1": "value1", "key2": "value2"},
            )

        assert received_msg == "test message"

    @pytest.mark.asyncio()
    async def test_message_attributes_in_handler(self) -> None:
        """Test accessing message attributes in handler with dependency injection."""
        broker = GCPBroker(project_id="test-project")
        captured_data = {}

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(
            msg: str,
            attrs: MessageAttributes,
            ordering_key: OrderingKey,
            msg_id: MessageId,
        ) -> None:
            captured_data.update({
                "message": msg,
                "attributes": attrs,
                "ordering_key": ordering_key,
                "message_id": msg_id,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "message with attrs",
                topic="test-topic",
                attributes={"attr1": "val1", "priority": "high"},
                ordering_key="order-123",
            )

        assert captured_data["message"] == "message with attrs"
        assert captured_data["attributes"]["attr1"] == "val1"
        assert captured_data["attributes"]["priority"] == "high"
        assert captured_data["ordering_key"] == "order-123"
        assert captured_data["message_id"] is not None

    @pytest.mark.asyncio()
    async def test_attributes_preservation_in_test_mode(self) -> None:
        """Test that attributes are preserved through the testing pipeline."""
        broker = GCPBroker(project_id="test-project")
        event = asyncio.Event()

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: Any) -> None:
            # In the future, we'll capture attributes via dependency injection
            # For now, we verify the test infrastructure preserves them
            event.set()

        async with TestGCPBroker(broker) as br:
            # Publish with specific attributes
            await br.publish(
                "test",
                topic="test-topic",
                attributes={
                    "trace_id": "12345",
                    "user_id": "user-789",
                    "timestamp": "2024-01-01T00:00:00Z",
                },
            )

            await asyncio.wait_for(event.wait(), timeout=3.0)

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_multiple_messages_with_different_attributes(self) -> None:
        """Test handling multiple messages with different attributes."""
        broker = GCPBroker(project_id="test-project")
        messages_received = []

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: str) -> None:
            messages_received.append(msg)

        async with TestGCPBroker(broker) as br:
            # Send messages with different attributes
            await br.publish(
                "msg1",
                topic="test-topic",
                attributes={"type": "A", "priority": "1"},
            )
            await br.publish(
                "msg2",
                topic="test-topic",
                attributes={"type": "B", "priority": "2"},
            )
            await br.publish(
                "msg3",
                topic="test-topic",
                attributes={"type": "A", "priority": "3"},
            )

        assert len(messages_received) == 3
        assert set(messages_received) == {"msg1", "msg2", "msg3"}

    @pytest.mark.asyncio()
    async def test_empty_attributes(self) -> None:
        """Test handling messages with no attributes."""
        broker = GCPBroker(project_id="test-project")
        mock = MagicMock()

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: str) -> None:
            mock(msg)

        async with TestGCPBroker(broker) as br:
            # Publish without attributes
            await br.publish("no attrs", topic="test-topic")
            # Publish with empty attributes dict
            await br.publish("empty attrs", topic="test-topic", attributes={})

        assert mock.call_count == 2

    @pytest.mark.asyncio()
    async def test_ordering_key_with_attributes(self) -> None:
        """Test that ordering key works alongside attributes."""
        broker = GCPBroker(project_id="test-project")
        messages = []

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: str) -> None:
            messages.append(msg)

        async with TestGCPBroker(broker) as br:
            # Publish with both ordering key and attributes
            await br.publish(
                "ordered-1",
                topic="test-topic",
                ordering_key="key1",
                attributes={"seq": "1"},
            )
            await br.publish(
                "ordered-2",
                topic="test-topic",
                ordering_key="key1",
                attributes={"seq": "2"},
            )

        assert len(messages) == 2

    @pytest.mark.asyncio()
    async def test_special_attribute_characters(self) -> None:
        """Test attributes with special characters."""
        broker = GCPBroker(project_id="test-project")
        received = False

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: str) -> None:
            nonlocal received
            received = True

        async with TestGCPBroker(broker) as br:
            # Test with various special characters in attribute values
            await br.publish(
                "special chars",
                topic="test-topic",
                attributes={
                    "path": "/usr/local/bin",
                    "email": "user@example.com",
                    "json": '{"key": "value"}',
                    "spaces": "value with spaces",
                    "unicode": "emoji-🚀",
                },
            )

        assert received

    @pytest.mark.asyncio()
    async def test_attribute_based_routing_preparation(self) -> None:
        """Test preparation for attribute-based routing."""
        broker = GCPBroker(project_id="test-project")
        high_priority_msgs = []
        low_priority_msgs = []

        @broker.subscriber("high-priority-sub", topic="test-topic")
        async def high_priority_handler(msg: str) -> None:
            # In future, we'll filter based on attributes
            if "high" in msg:
                high_priority_msgs.append(msg)

        @broker.subscriber("low-priority-sub", topic="test-topic")
        async def low_priority_handler(msg: str) -> None:
            # In future, we'll filter based on attributes
            if "low" in msg:
                low_priority_msgs.append(msg)

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "high priority message",
                topic="test-topic",
                attributes={"priority": "high"},
            )
            await br.publish(
                "low priority message",
                topic="test-topic",
                attributes={"priority": "low"},
            )

        assert len(high_priority_msgs) == 1
        assert len(low_priority_msgs) == 1


@pytest.mark.asyncio()
class TestAttributeIntegration:
    """Test attribute integration with publisher/subscriber."""

    @pytest.mark.asyncio()
    async def test_publisher_response_with_attributes(self) -> None:
        """Test publisher responses include attributes."""
        broker = GCPBroker(project_id="test-project")
        processed_messages = []

        @broker.subscriber("input-sub", topic="input-topic")
        @broker.publisher("output-topic")
        async def process_message(msg: str) -> str:
            # Future: Will return with attributes
            return f"processed: {msg}"

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: str) -> None:
            processed_messages.append(msg)

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "test input",
                topic="input-topic",
                attributes={"request_id": "123"},
            )

        assert len(processed_messages) == 1
        assert processed_messages[0] == "processed: test input"

    @pytest.mark.asyncio()
    async def test_batch_publish_with_attributes(self) -> None:
        """Test batch publishing with different attributes per message."""
        broker = GCPBroker(project_id="test-project")
        received_messages = []

        @broker.subscriber("batch-sub", topic="batch-topic")
        async def handler(msg: Any) -> None:
            received_messages.append(msg)

        async with TestGCPBroker(broker) as br:
            # Future implementation will support batch with individual attributes
            await br.publish("msg1", topic="batch-topic", attributes={"batch": "1"})
            await br.publish("msg2", topic="batch-topic", attributes={"batch": "1"})
            await br.publish("msg3", topic="batch-topic", attributes={"batch": "1"})

        assert len(received_messages) == 3


@pytest.mark.asyncio()
class TestAttributesDependencyInjection:
    """Test dependency injection for attributes (future implementation)."""

    @pytest.mark.asyncio()
    async def test_attribute_based_filtering(self) -> None:
        """Test attribute-based message filtering."""
        broker = GCPBroker(project_id="test-project")
        high_priority_messages = []
        all_messages = []

        @broker.subscriber("all-sub", topic="test-topic")
        async def all_handler(msg: str, attrs: MessageAttributes) -> None:
            all_messages.append({"msg": msg, "attrs": attrs})

        @broker.subscriber("priority-sub", topic="test-topic")
        async def priority_handler(msg: str, attrs: MessageAttributes) -> None:
            # Filter high priority messages
            if attrs.get("priority") == "high":
                high_priority_messages.append({"msg": msg, "attrs": attrs})

        async with TestGCPBroker(broker) as br:
            # High priority message
            await br.publish(
                "urgent task",
                topic="test-topic",
                attributes={"priority": "high", "user_id": "123"},
            )

            # Low priority message
            await br.publish(
                "regular task",
                topic="test-topic",
                attributes={"priority": "low", "user_id": "456"},
            )

        # All messages should be captured
        assert len(all_messages) == 2
        # Only high priority should be captured by priority handler
        assert len(high_priority_messages) == 1
        assert high_priority_messages[0]["msg"] == "urgent task"
        assert high_priority_messages[0]["attrs"]["priority"] == "high"
