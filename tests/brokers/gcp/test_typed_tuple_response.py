"""Tests for GCP Pub/Sub typed tuple response functionality."""

import pytest

from faststream.gcp import (
    GCPBroker,
    MessageAttributes,
    ResponseAttributes,
    ResponseOrderingKey,
)
from faststream.gcp.testing import TestGCPBroker


class TestTypedTupleResponse:
    """Test returning tuples with explicit type markers."""

    @pytest.mark.asyncio()
    async def test_response_attributes_marker(self) -> None:
        """Test using ResponseAttributes marker in tuple."""
        broker = GCPBroker(project_id="test-project")
        output_received = {}

        @broker.subscriber("input-sub", topic="input-topic")
        @broker.publisher("output-topic")
        async def handler(msg: str, attrs: MessageAttributes) -> tuple:
            # Use explicit ResponseAttributes marker
            return "processed", ResponseAttributes({
                "status": "success",
                "original_len": str(len(msg)),
            })

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: str, attrs: MessageAttributes) -> None:
            output_received.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish("test", topic="input-topic")

        assert output_received["message"] == "processed"
        assert output_received["attributes"]["status"] == "success"
        assert output_received["attributes"]["original_len"] == "4"

    @pytest.mark.asyncio()
    async def test_response_ordering_key_marker(self) -> None:
        """Test using ResponseOrderingKey marker in tuple."""
        broker = GCPBroker(project_id="test-project")
        output_received = {}

        @broker.subscriber("input-sub", topic="input-topic")
        @broker.publisher("output-topic")
        async def handler(msg: str, attrs: MessageAttributes) -> tuple:
            user_id = attrs.get("user_id", "unknown")
            # Use explicit ResponseOrderingKey marker
            return f"user-{user_id}: {msg}", ResponseOrderingKey(f"user-{user_id}")

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: str, attrs: MessageAttributes) -> None:
            output_received.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "hello",
                topic="input-topic",
                attributes={"user_id": "123"},
            )

        assert output_received["message"] == "user-123: hello"

    @pytest.mark.asyncio()
    async def test_both_markers_any_order(self) -> None:
        """Test using both markers in different orders."""
        broker = GCPBroker(project_id="test-project")
        results = []

        @broker.subscriber("input1-sub", topic="input1-topic")
        @broker.publisher("output-topic")
        async def handler1(msg: str) -> tuple:
            # Order: message, attributes, ordering key
            return (
                "msg1",
                ResponseAttributes({"handler": "1"}),
                ResponseOrderingKey("key1"),
            )

        @broker.subscriber("input2-sub", topic="input2-topic")
        @broker.publisher("output-topic")
        async def handler2(msg: str) -> tuple:
            # Order: attributes, message, ordering key
            return (
                ResponseAttributes({"handler": "2"}),
                "msg2",
                ResponseOrderingKey("key2"),
            )

        @broker.subscriber("input3-sub", topic="input3-topic")
        @broker.publisher("output-topic")
        async def handler3(msg: str) -> tuple:
            # Order: ordering key, attributes, message
            return (
                ResponseOrderingKey("key3"),
                ResponseAttributes({"handler": "3"}),
                "msg3",
            )

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: str, attrs: MessageAttributes) -> None:
            results.append({
                "message": msg,
                "handler": attrs.get("handler"),
            })

        async with TestGCPBroker(broker) as br:
            await br.publish("test", topic="input1-topic")
            await br.publish("test", topic="input2-topic")
            await br.publish("test", topic="input3-topic")

        assert len(results) == 3
        assert results[0] == {"message": "msg1", "handler": "1"}
        assert results[1] == {"message": "msg2", "handler": "2"}
        assert results[2] == {"message": "msg3", "handler": "3"}

    @pytest.mark.asyncio()
    async def test_complex_message_body_with_markers(self) -> None:
        """Test complex message bodies with type markers."""
        broker = GCPBroker(project_id="test-project")
        output_received = {}

        @broker.subscriber("input-sub", topic="input-topic")
        @broker.publisher("output-topic")
        async def handler(msg: dict, attrs: MessageAttributes) -> tuple:
            # Complex dict as message body, explicit attributes
            return (
                {"processed": msg, "count": len(msg)},
                ResponseAttributes({
                    "items": str(len(msg)),
                    "status": "processed",
                }),
                ResponseOrderingKey("complex-key"),
            )

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: dict, attrs: MessageAttributes) -> None:
            output_received.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                {"a": 1, "b": 2, "c": 3},
                topic="input-topic",
            )

        assert output_received["message"]["count"] == 3
        assert output_received["attributes"]["items"] == "3"
        assert output_received["attributes"]["status"] == "processed"

    @pytest.mark.asyncio()
    async def test_response_attributes_validation(self) -> None:
        """Test ResponseAttributes validates string keys/values."""
        # Valid attributes
        valid = ResponseAttributes({"key": "value", "another": "one"})
        assert dict(valid) == {"key": "value", "another": "one"}

        # Invalid attributes should raise TypeError
        with pytest.raises(TypeError, match="must have string keys and values"):
            ResponseAttributes({"key": 123})  # Non-string value

        with pytest.raises(TypeError, match="must have string keys and values"):
            ResponseAttributes({123: "value"})  # Non-string key

    @pytest.mark.asyncio()
    async def test_response_ordering_key_validation(self) -> None:
        """Test ResponseOrderingKey validates non-empty string."""
        # Valid ordering key
        valid = ResponseOrderingKey("user-123")
        assert str(valid) == "user-123"

        # Empty string should raise ValueError
        with pytest.raises(ValueError, match="cannot be empty"):
            ResponseOrderingKey("")

    @pytest.mark.asyncio()
    async def test_optional_components(self) -> None:
        """Test tuples with only some components."""
        broker = GCPBroker(project_id="test-project")
        results = []

        @broker.subscriber("input1-sub", topic="input1")
        @broker.publisher("output")
        async def only_attributes(msg: str) -> tuple:
            # Only attributes, no ordering key
            return msg + "-attrs", ResponseAttributes({"has": "attrs"})

        @broker.subscriber("input2-sub", topic="input2")
        @broker.publisher("output")
        async def only_ordering(msg: str) -> tuple:
            # Only ordering key, no attributes
            return msg + "-order", ResponseOrderingKey("order-key")

        @broker.subscriber("output-sub", topic="output")
        async def output_handler(msg: str, attrs: MessageAttributes) -> None:
            results.append({
                "message": msg,
                "has_attrs": "has" in attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish("test", topic="input1")
            await br.publish("test", topic="input2")

        assert len(results) == 2
        assert results[0]["message"] == "test-attrs"
        assert results[0]["has_attrs"] is True
        assert results[1]["message"] == "test-order"

    @pytest.mark.asyncio()
    async def test_plain_tuple_fallback(self) -> None:
        """Test that plain tuples without type markers fall back to normal response."""
        broker = GCPBroker(project_id="test-project")
        output_received = {}

        @broker.subscriber("input-sub", topic="input-topic")
        @broker.publisher("output-topic")
        async def handler(msg: str) -> tuple:
            # Plain tuple without type markers - should be treated as normal response
            return ("message", {"not": "attributes"}, "not-ordering")

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: tuple, attrs: MessageAttributes) -> None:
            output_received.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish("test", topic="input-topic")

        # The whole tuple should be treated as the message since no type markers
        assert output_received["message"] == (
            "message",
            {"not": "attributes"},
            "not-ordering",
        )
        assert isinstance(output_received["attributes"], dict)
