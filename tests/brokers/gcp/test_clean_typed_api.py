"""Test that the clean typed tuple API is working as intended."""

import pytest

from faststream.gcp import (
    GCPBroker,
    MessageAttributes,
    ResponseAttributes,
    ResponseOrderingKey,
)
from faststream.gcp.testing import TestGCPBroker


class TestCleanTypedAPI:
    """Test the clean, type-based tuple response API."""

    @pytest.mark.asyncio()
    async def test_only_type_markers_work(self) -> None:
        """Test that only explicit type markers create GCPResponse."""
        broker = GCPBroker(project_id="test-project")
        results = []

        @broker.subscriber("typed-sub", topic="typed")
        @broker.publisher("output")
        async def typed_handler(msg: str) -> tuple:
            # Uses type markers - should work
            return msg, ResponseAttributes({"typed": "true"})

        @broker.subscriber("plain-sub", topic="plain")
        @broker.publisher("output")
        async def plain_handler(msg: str) -> tuple:
            # Plain tuple - should be treated as single message
            return ("msg", {"not": "attrs"})

        @broker.subscriber("output-sub", topic="output")
        async def output_handler(msg, attrs: MessageAttributes) -> None:
            results.append({
                "message": msg,
                "is_typed": attrs.get("typed") == "true",
                "message_type": type(msg).__name__,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish("test", topic="typed")
            await br.publish("test", topic="plain")

        assert len(results) == 2

        # First result: typed handler with ResponseAttributes
        assert results[0]["message"] == "test"
        assert results[0]["is_typed"] is True
        assert results[0]["message_type"] == "str"

        # Second result: plain tuple treated as single message (serialized as list)
        assert results[1]["message"] == ["msg", {"not": "attrs"}]
        assert results[1]["is_typed"] is False
        assert results[1]["message_type"] == "list"

    @pytest.mark.asyncio()
    async def test_type_marker_validation_enforced(self) -> None:
        """Test that type marker validation is enforced."""
        # These should work
        valid_attrs = ResponseAttributes({"key": "value"})
        valid_key = ResponseOrderingKey("order-123")

        assert dict(valid_attrs) == {"key": "value"}
        assert str(valid_key) == "order-123"

        # These should fail
        with pytest.raises(TypeError):
            ResponseAttributes({"key": 123})  # Non-string value

        with pytest.raises(TypeError):
            ResponseAttributes({123: "value"})  # Non-string key

        with pytest.raises(ValueError):  # noqa: PT011
            ResponseOrderingKey("")  # Empty string

    @pytest.mark.asyncio()
    async def test_only_message_plus_markers(self) -> None:
        """Test that tuples need message plus at least one marker."""
        broker = GCPBroker(project_id="test-project")
        results = []

        @broker.subscriber("valid-sub", topic="valid")
        @broker.publisher("output")
        async def valid_handler(msg: str) -> tuple:
            # Message + one marker = valid
            return "processed", ResponseAttributes({"status": "ok"})

        @broker.subscriber("invalid-sub", topic="invalid")
        @broker.publisher("output")
        async def invalid_handler(msg: str) -> tuple:
            # Just message, no markers = treated as plain tuple
            return ("just", "strings")

        @broker.subscriber("output-sub", topic="output")
        async def output_handler(msg, attrs: MessageAttributes) -> None:
            results.append({
                "message": msg,
                "has_status": "status" in attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish("test", topic="valid")
            await br.publish("test", topic="invalid")

        assert len(results) == 2
        assert results[0]["message"] == "processed"
        assert results[0]["has_status"] is True
        assert results[1]["message"] == [
            "just",
            "strings",
        ]  # Plain tuple (serialized as list)
        assert results[1]["has_status"] is False

    @pytest.mark.asyncio()
    async def test_order_independence_guaranteed(self) -> None:
        """Test that order truly doesn't matter with type markers."""
        broker = GCPBroker(project_id="test-project")
        results = []

        @broker.subscriber("test0-sub", topic="test0")
        @broker.publisher("output")
        async def handler0(msg: str) -> tuple:
            return ("msg", ResponseAttributes({"pos": "1"}), ResponseOrderingKey("key"))

        @broker.subscriber("test1-sub", topic="test1")
        @broker.publisher("output")
        async def handler1(msg: str) -> tuple:
            return (ResponseAttributes({"pos": "2"}), "msg", ResponseOrderingKey("key"))

        @broker.subscriber("test2-sub", topic="test2")
        @broker.publisher("output")
        async def handler2(msg: str) -> tuple:
            return (ResponseOrderingKey("key"), ResponseAttributes({"pos": "3"}), "msg")

        @broker.subscriber("test3-sub", topic="test3")
        @broker.publisher("output")
        async def handler3(msg: str) -> tuple:
            return (ResponseOrderingKey("key"), "msg", ResponseAttributes({"pos": "4"}))

        @broker.subscriber("output-sub", topic="output")
        async def output_handler(msg: str, attrs: MessageAttributes) -> None:
            results.append({
                "message": msg,
                "pos": attrs.get("pos"),
            })

        async with TestGCPBroker(broker) as br:
            await br.publish("test", topic="test0")
            await br.publish("test", topic="test1")
            await br.publish("test", topic="test2")
            await br.publish("test", topic="test3")

        assert len(results) == 4
        # All should have same message, different pos values
        for i, result in enumerate(results, 1):
            assert result["message"] == "msg"
            assert result["pos"] == str(i)
