"""Tests for GCP Pub/Sub message attributes dependency injection."""

import pytest

from faststream.gcp import GCPBroker, MessageAttributes, MessageId, OrderingKey
from faststream.gcp.testing import TestGCPBroker


class TestAttributesDependencyInjection:
    """Test dependency injection for message attributes."""

    @pytest.mark.asyncio()
    async def test_message_attributes_injection(self) -> None:
        """Test basic MessageAttributes dependency injection."""
        broker = GCPBroker(project_id="test-project")
        captured_data = {}

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: str, attrs: MessageAttributes) -> None:
            captured_data.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "test message",
                topic="test-topic",
                attributes={"user_id": "123", "priority": "high"},
            )

        assert captured_data["message"] == "test message"
        assert captured_data["attributes"]["user_id"] == "123"
        assert captured_data["attributes"]["priority"] == "high"

    @pytest.mark.asyncio()
    async def test_ordering_key_injection(self) -> None:
        """Test OrderingKey dependency injection."""
        broker = GCPBroker(project_id="test-project")
        captured_data = {}

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: str, ordering_key: OrderingKey) -> None:
            captured_data.update({
                "message": msg,
                "ordering_key": ordering_key,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "ordered message",
                topic="test-topic",
                ordering_key="order-123",
            )

        assert captured_data["message"] == "ordered message"
        assert captured_data["ordering_key"] == "order-123"

    @pytest.mark.asyncio()
    async def test_message_id_injection(self) -> None:
        """Test MessageId dependency injection."""
        broker = GCPBroker(project_id="test-project")
        captured_data = {}

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: str, msg_id: MessageId) -> None:
            captured_data.update({
                "message": msg,
                "message_id": msg_id,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish("test message", topic="test-topic")

        assert captured_data["message"] == "test message"
        assert captured_data["message_id"] is not None

    @pytest.mark.asyncio()
    async def test_multiple_attributes_injection(self) -> None:
        """Test injecting multiple attribute types together."""
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
                "complete message",
                topic="test-topic",
                attributes={"type": "order", "priority": "high"},
                ordering_key="order-456",
            )

        assert captured_data["message"] == "complete message"
        assert captured_data["attributes"]["type"] == "order"
        assert captured_data["attributes"]["priority"] == "high"
        assert captured_data["ordering_key"] == "order-456"
        assert captured_data["message_id"] is not None

    @pytest.mark.asyncio()
    async def test_empty_attributes_injection(self) -> None:
        """Test behavior with empty or missing attributes."""
        broker = GCPBroker(project_id="test-project")
        captured_data = {}

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: str, attrs: MessageAttributes) -> None:
            captured_data.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            # Publish without attributes
            await br.publish("no attrs", topic="test-topic")

        assert captured_data["message"] == "no attrs"
        assert isinstance(captured_data["attributes"], dict)

    @pytest.mark.asyncio()
    async def test_none_ordering_key_injection(self) -> None:
        """Test OrderingKey injection when no ordering key provided."""
        broker = GCPBroker(project_id="test-project")
        captured_data = {}

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: str, ordering_key: OrderingKey) -> None:
            captured_data.update({
                "message": msg,
                "ordering_key": ordering_key,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish("no ordering", topic="test-topic")

        assert captured_data["message"] == "no ordering"
        assert captured_data["ordering_key"] in {None, ""}

    @pytest.mark.asyncio()
    async def test_attributes_type_safety(self) -> None:
        """Test that attributes are properly typed as Dict[str, str]."""
        broker = GCPBroker(project_id="test-project")
        captured_attrs = None

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(msg: str, attrs: MessageAttributes) -> None:
            nonlocal captured_attrs
            captured_attrs = attrs

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "typed message",
                topic="test-topic",
                attributes={"string_key": "string_value", "number_like": "123"},
            )

        assert isinstance(captured_attrs, dict)
        assert all(isinstance(k, str) for k, _ in captured_attrs.items())
        assert all(isinstance(v, str) for v in captured_attrs.values())
        assert captured_attrs["string_key"] == "string_value"
        assert captured_attrs["number_like"] == "123"

    @pytest.mark.asyncio()
    async def test_mixed_parameters_injection(self) -> None:
        """Test mixing DI attributes with regular parameters."""
        broker = GCPBroker(project_id="test-project")
        captured_data = {}

        @broker.subscriber("test-sub", topic="test-topic")
        async def handler(
            msg: str,
            attrs: MessageAttributes,
            ordering_key: OrderingKey = None,  # Default parameter
        ) -> None:
            captured_data.update({
                "message": msg,
                "attributes": attrs,
                "ordering_key": ordering_key,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "mixed params",
                topic="test-topic",
                attributes={"feature": "enabled"},
                ordering_key="mixed-order",
            )

        assert captured_data["message"] == "mixed params"
        assert captured_data["attributes"]["feature"] == "enabled"
        assert captured_data["ordering_key"] == "mixed-order"
