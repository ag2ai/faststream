import pytest
from unittest.mock import MagicMock

from faststream.kafka.parser import AioKafkaBatchParser, AioKafkaParser
from faststream.kafka.message import KafkaMessage
from tests.brokers.base.parser import CustomParserTestcase

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.connected()
class TestCustomParser(KafkaTestcaseConfig, CustomParserTestcase):
    pass


class TestNonUtf8KafkaHeaders:
    """Tests for non-UTF-8 header handling in AioKafkaParser / AioKafkaBatchParser.

    Covers issue:
    - https://github.com/ag2ai/faststream/issues/2458 (UnicodeDecodeError on non-UTF-8 headers)
    """

    def _make_record(self, headers):
        record = MagicMock()
        record.headers = headers
        record.value = b"body"
        record.offset = 0
        record.timestamp = 0
        record.topic = "test-topic"
        record.consumer = MagicMock()
        return record

    def _make_parser(self):
        return AioKafkaParser(msg_class=KafkaMessage, regex=None)

    def _make_batch_parser(self):
        return AioKafkaBatchParser(msg_class=KafkaMessage, regex=None)

    @pytest.mark.asyncio
    async def test_non_utf8_header_does_not_raise(self):
        """parse_message must not raise UnicodeDecodeError on invalid UTF-8 bytes."""
        parser = self._make_parser()
        record = self._make_record([("trash_header", b"\xc3\x28")])
        result = await parser.parse_message(record)
        assert "trash_header" in result.headers

    @pytest.mark.asyncio
    async def test_non_utf8_header_decoded_with_replace(self):
        """Non-UTF-8 header bytes should be decoded with errors='replace'."""
        parser = self._make_parser()
        record = self._make_record([("key", b"\xc3\x28")])
        result = await parser.parse_message(record)
        assert isinstance(result.headers["key"], str)

    @pytest.mark.asyncio
    async def test_valid_utf8_decoded_correctly(self):
        """Valid UTF-8 headers should decode without modification."""
        parser = self._make_parser()
        record = self._make_record([
            ("reply_to", b"my-topic"),
            ("content-type", b"application/json"),
            ("correlation_id", b"uuid-123"),
        ])
        result = await parser.parse_message(record)
        assert result.reply_to == "my-topic"
        assert result.content_type == "application/json"
        assert result.correlation_id == "uuid-123"

    @pytest.mark.asyncio
    async def test_reply_to_defaults_to_empty_string(self):
        """reply_to must default to '' when header is absent."""
        parser = self._make_parser()
        record = self._make_record([])
        result = await parser.parse_message(record)
        assert result.reply_to == ""

    @pytest.mark.asyncio
    async def test_batch_non_utf8_header_does_not_raise(self):
        """parse_batch must not raise on invalid UTF-8."""
        parser = self._make_batch_parser()
        records = tuple([
            self._make_record([("trash_header", b"\xc3\x28")]),
            self._make_record([("other", b"valid")]),
        ])
        result = await parser.parse_batch(records)
        assert isinstance(result.reply_to, str)

    @pytest.mark.asyncio
    async def test_batch_headers_decoded_with_replace(self):
        """All batch_headers should be decoded with errors='replace'."""
        parser = self._make_batch_parser()
        records = tuple([
            self._make_record([("key", b"\xc3\x28")]),
        ])
        result = await parser.parse_batch(records)
        assert isinstance(result.batch_headers[0]["key"], str)
