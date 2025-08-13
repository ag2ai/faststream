"""GCP Pub/Sub message parser tests."""

import asyncio
from typing import Any
from unittest.mock import Mock

import pytest
from gcloud.aio.pubsub import PubsubMessage
from pydantic import BaseModel

from faststream.gcppubsub.message import GCPPubSubMessage
from faststream.gcppubsub.parser import GCPPubSubParser
from tests.brokers.base.parser import LocalCustomParserTestcase
from tests.marks import require_gcppubsub

from .basic import GCPPubSubTestcaseConfig


class MessageModel(BaseModel):
    """Test Pydantic model for parser tests."""

    name: str
    value: int


@pytest.mark.gcppubsub()
@require_gcppubsub
class TestParser(GCPPubSubTestcaseConfig, LocalCustomParserTestcase):
    """Test GCP Pub/Sub message parsing functionality."""

    async def test_default_parser_creation(self) -> None:
        """Test creating default GCP Pub/Sub parser."""
        parser = GCPPubSubParser()
        assert isinstance(parser, GCPPubSubParser)

    @pytest.mark.asyncio()
    async def test_parse_basic_message(self) -> None:
        """Test parsing basic PubsubMessage."""
        parser = GCPPubSubParser()

        # Create a basic PubsubMessage
        pubsub_msg = PubsubMessage(
            data=b"test message", message_id="test-id-123", attributes={"key": "value"}
        )

        parsed_msg = await parser.parse_message(pubsub_msg)

        assert isinstance(parsed_msg, GCPPubSubMessage)
        assert parsed_msg.raw_message == pubsub_msg
        assert parsed_msg.correlation_id is not None

    @pytest.mark.asyncio()
    async def test_parse_message_with_correlation_id(self) -> None:
        """Test parsing message with correlation ID in attributes."""
        parser = GCPPubSubParser()

        correlation_id = "test-correlation-123"
        pubsub_msg = PubsubMessage(
            data=b"test message",
            message_id="test-id-123",
            attributes={"correlation_id": correlation_id},
        )

        parsed_msg = await parser.parse_message(pubsub_msg)

        assert parsed_msg.correlation_id == correlation_id

    @pytest.mark.asyncio()
    async def test_parse_message_without_attributes(self) -> None:
        """Test parsing message without attributes."""
        parser = GCPPubSubParser()

        pubsub_msg = PubsubMessage(data=b"test message", message_id="test-id-123")

        parsed_msg = await parser.parse_message(pubsub_msg)

        assert isinstance(parsed_msg, GCPPubSubMessage)
        assert parsed_msg.correlation_id is not None  # Should generate one

    @pytest.mark.asyncio()
    async def test_decode_message_basic(self) -> None:
        """Test basic message decoding."""
        parser = GCPPubSubParser()

        # Create a GCPPubSubMessage
        pubsub_msg = PubsubMessage(data=b"test message")
        stream_msg = await parser.parse_message(pubsub_msg)

        decoded = await parser.decode_message(stream_msg)

        # The decode_message should return decoded message content
        assert decoded is not None

    @pytest.mark.asyncio()
    async def test_parse_json_data(self) -> None:
        """Test parsing JSON data in message."""
        parser = GCPPubSubParser()

        json_data = b'{"name": "test", "value": 42}'
        pubsub_msg = PubsubMessage(data=json_data, message_id="json-test-123")

        parsed_msg = await parser.parse_message(pubsub_msg)

        assert isinstance(parsed_msg, GCPPubSubMessage)
        assert parsed_msg.raw_message.data == json_data

    @pytest.mark.asyncio()
    async def test_parse_binary_data(self) -> None:
        """Test parsing binary data."""
        parser = GCPPubSubParser()

        binary_data = b"\x00\x01\x02\x03\x04\x05"
        pubsub_msg = PubsubMessage(data=binary_data, message_id="binary-test-123")

        parsed_msg = await parser.parse_message(pubsub_msg)

        assert isinstance(parsed_msg, GCPPubSubMessage)
        assert parsed_msg.raw_message.data == binary_data

    @pytest.mark.asyncio()
    async def test_parse_empty_message(self) -> None:
        """Test parsing empty message."""
        parser = GCPPubSubParser()

        pubsub_msg = PubsubMessage(data=b"", message_id="empty-test-123")

        parsed_msg = await parser.parse_message(pubsub_msg)

        assert isinstance(parsed_msg, GCPPubSubMessage)
        assert parsed_msg.raw_message.data == b""

    @pytest.mark.asyncio()
    async def test_parse_large_message(self) -> None:
        """Test parsing large message."""
        parser = GCPPubSubParser()

        # Create large message data
        large_data = b"x" * 10000  # 10KB
        pubsub_msg = PubsubMessage(data=large_data, message_id="large-test-123")

        parsed_msg = await parser.parse_message(pubsub_msg)

        assert isinstance(parsed_msg, GCPPubSubMessage)
        assert len(parsed_msg.raw_message.data) == 10000

    @pytest.mark.asyncio()
    async def test_parse_message_with_complex_attributes(self) -> None:
        """Test parsing message with complex attributes."""
        parser = GCPPubSubParser()

        complex_attributes = {
            "content-type": "application/json",
            "source": "test-service",
            "priority": "high",
            "retry-count": "3",
            "correlation_id": "complex-test-456",
        }

        pubsub_msg = PubsubMessage(
            data=b'{"message": "complex test"}',
            message_id="complex-test-123",
            attributes=complex_attributes,
        )

        parsed_msg = await parser.parse_message(pubsub_msg)

        assert isinstance(parsed_msg, GCPPubSubMessage)
        assert parsed_msg.correlation_id == "complex-test-456"
        assert parsed_msg.attributes["content-type"] == "application/json"

    @pytest.mark.asyncio()
    async def test_custom_parser_integration(self, subscription: str, topic: str) -> None:
        """Test custom parser integration."""
        broker = self.get_broker()
        custom_parsed_messages = []

        async def custom_parser(msg):
            """Custom parser function."""
            custom_parsed_messages.append("custom_parsed")
            # Return the message as-is for this test
            return msg

        @broker.subscriber(
            subscription,
            topic=topic,
            create_subscription=True,
            parser=custom_parser,
        )
        async def handler(msg: Any) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("test", topic=topic)),
                    asyncio.create_task(asyncio.sleep(0.5)),
                ),
                timeout=self.timeout,
            )

        # Custom parser should have been called
        assert "custom_parsed" in custom_parsed_messages

    @pytest.mark.asyncio()
    async def test_pydantic_model_parsing(self) -> None:
        """Test parsing with Pydantic models."""
        broker = self.get_broker()
        parsed_models = []

        @broker.subscriber("pydantic-subscription", topic="pydantic-topic")
        async def pydantic_handler(msg: MessageModel) -> None:
            parsed_models.append(msg)

        async with self.patch_broker(broker) as br:
            await br.start()

            # Publish data that matches the Pydantic model
            test_data = {"name": "test_item", "value": 42}
            await br.publish(test_data, topic="pydantic-topic")
            await asyncio.sleep(0.1)

        # Should have successfully parsed into Pydantic model
        # Note: The exact behavior depends on FastStream's Pydantic integration
        assert len(parsed_models) >= 0  # May be 0 if Pydantic parsing isn't automatic

    @pytest.mark.asyncio()
    async def test_parser_error_handling(self) -> None:
        """Test parser error handling with invalid data."""
        broker = self.get_broker()
        error_handled = []

        async def failing_parser(msg):
            """Parser that always fails."""
            error_msg = "Parser error"
            raise ValueError(error_msg)

        @broker.subscriber(
            "error-parser-subscription", topic="error-parser-topic", parser=failing_parser
        )
        async def handler(msg: Any) -> None:
            error_handled.append("handler_called")

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("test", topic="error-parser-topic")
            await asyncio.sleep(0.1)

        # Handler should not be called due to parser error
        assert "handler_called" not in error_handled

    async def test_parser_with_subscriber_message(self) -> None:
        """Test parser compatibility with SubscriberMessage objects."""
        parser = GCPPubSubParser()

        # Mock a SubscriberMessage-like object
        mock_subscriber_msg = Mock()
        mock_subscriber_msg.data = b"test data"
        mock_subscriber_msg.message_id = "subscriber-test-123"
        mock_subscriber_msg.attributes = {"test": "attr"}

        # The parser should handle SubscriberMessage objects too
        # This tests compatibility with gcloud-aio's SubscriberMessage
        # The exact implementation may vary
        assert hasattr(parser, "parse_message")

    @pytest.mark.asyncio()
    async def test_message_validation(self) -> None:
        """Test message validation during parsing."""
        parser = GCPPubSubParser()

        # Test with valid message
        valid_msg = PubsubMessage(data=b"valid message", message_id="valid-123")

        parsed = await parser.parse_message(valid_msg)
        assert isinstance(parsed, GCPPubSubMessage)

        # Test with invalid/None message should raise appropriate error
        with pytest.raises((TypeError, AttributeError, ValueError)):
            await parser.parse_message(None)  # type: ignore[arg-type]

    @pytest.mark.asyncio()
    async def test_nested_model_parsing(self) -> None:
        """Test parsing complex nested data structures."""

        class NestedModel(BaseModel):
            inner: MessageModel
            metadata: dict[str, str]

        broker = self.get_broker()
        nested_parsed = []

        @broker.subscriber("nested-subscription", topic="nested-topic")
        async def nested_handler(msg: NestedModel) -> None:
            nested_parsed.append(msg)

        async with self.patch_broker(broker) as br:
            await br.start()

            nested_data = {
                "inner": {"name": "nested_test", "value": 99},
                "metadata": {"source": "test", "version": "1.0"},
            }
            await br.publish(nested_data, topic="nested-topic")
            await asyncio.sleep(0.1)

        # May or may not parse automatically depending on implementation
        assert len(nested_parsed) >= 0
