"""Tests for publishing messages with attributes."""

from typing import Any

import pytest

from faststream.gcp import GCPBroker, MessageAttributes
from faststream.gcp.testing import TestGCPBroker


class TestPublishingAttributes:
    """Test publishing messages with attributes."""

    @pytest.mark.asyncio()
    async def test_publish_with_attributes(self) -> None:
        """Test basic publishing with attributes."""
        broker = GCPBroker(project_id="test-project")
        received_data = {}

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: str, attrs: MessageAttributes) -> None:
            received_data.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "test message",
                topic="test-topic",
                attributes={
                    "user_id": "123",
                    "priority": "high",
                    "source": "api",
                },
            )

        assert received_data["message"] == "test message"
        assert received_data["attributes"]["user_id"] == "123"
        assert received_data["attributes"]["priority"] == "high"
        assert received_data["attributes"]["source"] == "api"

    @pytest.mark.asyncio()
    async def test_publisher_decorator_response_attributes(self) -> None:
        """Test @publisher decorator with response attributes."""
        broker = GCPBroker(project_id="test-project")
        input_received = None
        output_received = {}

        @broker.subscriber("input-sub", topic="input-topic")
        @broker.publisher("output-topic")
        async def process_message(msg: str, attrs: MessageAttributes) -> str:
            nonlocal input_received
            input_received = {"msg": msg, "attrs": attrs}
            # Return processed message - attributes will be handled separately
            return f"processed: {msg}"

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: str, attrs: MessageAttributes) -> None:
            output_received.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "input message",
                topic="input-topic",
                attributes={"request_id": "req-123", "user_id": "user-456"},
            )

        # Check input processing
        assert input_received["msg"] == "input message"
        assert input_received["attrs"]["request_id"] == "req-123"
        assert input_received["attrs"]["user_id"] == "user-456"

        # Check output
        assert output_received["message"] == "processed: input message"
        # Note: Currently no mechanism to add attributes to response

    @pytest.mark.asyncio()
    async def test_manual_publish_with_attributes_in_handler(self) -> None:
        """Test manually publishing with attributes from within a handler."""
        broker = GCPBroker(project_id="test-project")
        input_received = None
        output_received = {}

        @broker.subscriber("input-sub", topic="input-topic")
        async def process_message(msg: str, attrs: MessageAttributes) -> None:
            nonlocal input_received
            input_received = {"msg": msg, "attrs": attrs}

            # Manually publish with custom attributes
            await broker.publish(
                f"processed: {msg}",
                topic="output-topic",
                attributes={
                    "request_id": attrs.get("request_id"),
                    "processed_by": "handler",
                    "priority": "normal",
                },
            )

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: str, attrs: MessageAttributes) -> None:
            output_received.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "input message",
                topic="input-topic",
                attributes={"request_id": "req-123", "user_id": "user-456"},
            )

        # Check input processing
        assert input_received["msg"] == "input message"
        assert input_received["attrs"]["request_id"] == "req-123"

        # Check output with custom attributes
        assert output_received["message"] == "processed: input message"
        assert output_received["attributes"]["request_id"] == "req-123"
        assert output_received["attributes"]["processed_by"] == "handler"
        assert output_received["attributes"]["priority"] == "normal"

    @pytest.mark.asyncio()
    async def test_batch_publish_with_attributes(self) -> None:
        """Test batch publishing with attributes."""
        broker = GCPBroker(project_id="test-project")
        received_messages = []

        @broker.subscriber("batch-sub", topic="batch-topic")
        async def handler(msg: str, attrs: MessageAttributes) -> None:
            received_messages.append({
                "message": msg,
                "attributes": dict(attrs),
            })

        async with TestGCPBroker(broker) as br:
            # Note: Current batch publishing applies same attributes to all messages
            await br.publish_batch(
                ["msg1", "msg2", "msg3"],
                topic="batch-topic",
                attributes={"batch_id": "batch-123", "priority": "high"},
            )

        assert len(received_messages) == 3
        for i, received in enumerate(received_messages, 1):
            assert received["message"] == f"msg{i}"
            assert received["attributes"]["batch_id"] == "batch-123"
            assert received["attributes"]["priority"] == "high"

    @pytest.mark.asyncio()
    async def test_ordering_key_with_attributes(self) -> None:
        """Test publishing with both ordering key and attributes."""
        broker = GCPBroker(project_id="test-project")
        received_data = {}

        @broker.subscriber("order-sub", topic="order-topic")
        async def handler(msg: str, attrs: MessageAttributes) -> None:
            # We can't directly inject ordering key here due to current test setup
            # but we can verify the message structure
            received_data.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "ordered message",
                topic="order-topic",
                attributes={"sequence": "1", "group": "A"},
                ordering_key="group-A",
            )

        assert received_data["message"] == "ordered message"
        assert received_data["attributes"]["sequence"] == "1"
        assert received_data["attributes"]["group"] == "A"

    @pytest.mark.asyncio()
    async def test_publish_complex_attributes(self) -> None:
        """Test publishing with complex attribute scenarios."""
        broker = GCPBroker(project_id="test-project")
        received_data = {}

        @broker.subscriber("complex-sub", topic="complex-topic")
        async def handler(msg: dict[str, Any], attrs: MessageAttributes) -> None:
            received_data.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            # Test with complex message and rich attributes
            await br.publish(
                {"user": "john", "action": "login", "timestamp": "2024-01-01T12:00:00Z"},
                topic="complex-topic",
                attributes={
                    "trace_id": "trace-abc123",
                    "span_id": "span-def456",
                    "user_id": "user-789",
                    "tenant_id": "tenant-456",
                    "priority": "high",
                    "content_type": "application/json",
                    "source_system": "auth-service",
                    "version": "1.0",
                },
            )

        assert received_data["message"]["user"] == "john"
        assert received_data["message"]["action"] == "login"
        assert received_data["attributes"]["trace_id"] == "trace-abc123"
        assert received_data["attributes"]["span_id"] == "span-def456"
        assert received_data["attributes"]["user_id"] == "user-789"
        assert received_data["attributes"]["tenant_id"] == "tenant-456"
        assert received_data["attributes"]["priority"] == "high"
        assert received_data["attributes"]["source_system"] == "auth-service"
