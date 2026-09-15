import pytest
from unittest.mock import MagicMock

from faststream.confluent.parser import AsyncConfluentParser
from tests.brokers.base.parser import CustomParserTestcase

from .basic import ConfluentTestcaseConfig


@pytest.mark.connected()
@pytest.mark.confluent()
class TestCustomParser(ConfluentTestcaseConfig, CustomParserTestcase):
    pass


class TestNonUtf8ConfluentHeaders:
    """Tests for non-UTF-8 header handling in AsyncConfluentParser.

    Covers issues:
    - https://github.com/ag2ai/faststream/issues/2458 (UnicodeDecodeError on non-UTF-8 headers)
    - https://github.com/ag2ai/faststream/issues/2214 (support bytes headers)
    """

    def _make_message(self, headers):
        msg = MagicMock()
        msg.headers.return_value = headers
        msg.value.return_value = b"body"
        msg.offset.return_value = 0
        msg.timestamp.return_value = (0, 0)
        return msg

    def _make_batch(self, headers_list):
        messages = []
        for headers in headers_list:
            msg = MagicMock()
            msg.headers.return_value = headers
            msg.value.return_value = b"body"
            msg.offset.return_value = 0
            msg.timestamp.return_value = (0, 0)
            messages.append(msg)
        return tuple(messages)

    @pytest.mark.asyncio
    async def test_non_utf8_header_does_not_raise(self):
        """parse_message must not raise UnicodeDecodeError on invalid UTF-8 bytes."""
        parser = AsyncConfluentParser()
        msg = self._make_message([("trash_header", b"\xc3\x28")])
        result = await parser.parse_message(msg)
        assert "trash_header" in result.headers

    @pytest.mark.asyncio
    async def test_non_utf8_reply_to_decoded_with_replace(self):
        """reply_to with non-UTF-8 bytes should be decoded with errors='replace'."""
        parser = AsyncConfluentParser()
        msg = self._make_message([("reply_to", b"\xc3\x28")])
        result = await parser.parse_message(msg)
        assert isinstance(result.reply_to, str)

    @pytest.mark.asyncio
    async def test_reply_to_empty_string_when_missing(self):
        """reply_to must default to '' when header is absent."""
        parser = AsyncConfluentParser()
        msg = self._make_message([])
        result = await parser.parse_message(msg)
        assert result.reply_to == ""

    @pytest.mark.asyncio
    async def test_content_type_none_when_missing(self):
        """content_type must be None when header is absent."""
        parser = AsyncConfluentParser()
        msg = self._make_message([])
        result = await parser.parse_message(msg)
        assert result.content_type is None

    @pytest.mark.asyncio
    async def test_bytes_header_value_preserved_in_headers(self):
        """Issue #2214: raw bytes header values should be preserved in msg.headers."""
        import uuid
        parser = AsyncConfluentParser()
        event_id = uuid.uuid4().bytes
        msg = self._make_message([("event_id", event_id)])
        result = await parser.parse_message(msg)
        assert result.headers.get("event_id") == event_id

    @pytest.mark.asyncio
    async def test_valid_utf8_header_decoded_correctly(self):
        """Valid UTF-8 headers should be decoded correctly."""
        parser = AsyncConfluentParser()
        msg = self._make_message([
            ("reply_to", b"some-topic"),
            ("content-type", b"application/json"),
            ("correlation_id", b"abc-123"),
        ])
        result = await parser.parse_message(msg)
        assert result.reply_to == "some-topic"
        assert result.content_type == "application/json"
        assert result.correlation_id == "abc-123"

    @pytest.mark.asyncio
    async def test_none_header_value_handled(self):
        """Header with None value should not raise."""
        parser = AsyncConfluentParser()
        msg = self._make_message([("nullable_header", None)])
        result = await parser.parse_message(msg)
        assert result.headers.get("nullable_header") is None

    @pytest.mark.asyncio
    async def test_batch_non_utf8_header_does_not_raise(self):
        """parse_batch must not raise on non-UTF-8 headers."""
        parser = AsyncConfluentParser()
        messages = self._make_batch([
            [("trash_header", b"\xc3\x28")],
            [("other", b"valid")],
        ])
        result = await parser.parse_batch(messages)
        assert isinstance(result.reply_to, str)

    @pytest.mark.asyncio
    async def test_batch_bytes_headers_preserved(self):
        """Issue #2214: bytes header values preserved in batch_headers."""
        import uuid
        parser = AsyncConfluentParser()
        event_id = uuid.uuid4().bytes
        messages = self._make_batch([
            [("event_id", event_id)],
        ])
        result = await parser.parse_batch(messages)
        assert result.batch_headers[0].get("event_id") == event_id

    def test_decode_header_returns_none_for_missing_key(self):
        assert AsyncConfluentParser._decode_header({}, "missing") is None

    def test_decode_header_decodes_valid_bytes(self):
        assert AsyncConfluentParser._decode_header({"key": b"value"}, "key") == "value"

    def test_decode_header_replaces_invalid_bytes(self):
        result = AsyncConfluentParser._decode_header({"key": b"\xc3\x28"}, "key")
        assert isinstance(result, str)

    def test_decode_header_passthrough_str(self):
        assert AsyncConfluentParser._decode_header({"key": "already-str"}, "key") == "already-str"

    def test_decode_header_returns_none_for_none_value(self):
        assert AsyncConfluentParser._decode_header({"key": None}, "key") is None
