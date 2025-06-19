import pytest

from faststream._compat import json_dumps
from faststream.redis import RedisBroker
from faststream.redis.parser import RawMessage
from tests.brokers.base.parser import CustomParserTestcase


@pytest.mark.redis
class TestCustomParser(CustomParserTestcase):
    broker_class = RedisBroker


@pytest.mark.redis
class TestRedisRawMessage:
#     @pytest.mark.parametrize(
#         ("input", "should_be"),
#         [
#             (b"", b'\x00\x00\x00\x18{"correlation_id": "id"}'),
#             (
#                 "plain text",
#                 b'{"data": "plain text", "headers": {"correlation_id": "id", "content-type": "text/plain"}}',
#             ),
#             (
#                 {"data": "data"},
#                 b'{"data": "{\\"data\\": \\"data\\"}", "headers": {"correlation_id": "id", "content-type": "application/json"}}',
#             ),
#         ],
#     )
#     def test_raw_message_encode(self, input, should_be):
#         encoded = RawMessage.encode(
#             message=input, reply_to=None, headers=None, correlation_id="id"
#         )
#         assert encoded == should_be
#
#     @pytest.mark.parametrize(
#         ("input", "should_be"),
#         [
#             (b"", b""),
#             (b"plain text", b"plain text"),
#             (b"\x00\x00\x00\x02{}message", b"message"),
#         ],
#     )
#     def test_raw_message_parse(self, input, should_be):
#         parsed, _ = RawMessage.parse(input)
#         assert parsed == should_be
#
    @pytest.mark.parametrize(
        ("input", "should_be"),
        [
            ("", b""),
            ("plain text", b"plain text"),
            (
                {"id": "12345678" * 4, "date": "2021-01-01T00:00:00Z"},
                json_dumps({"id": "12345678" * 4, "date": "2021-01-01T00:00:00Z"}),
            ),
            (
                # UTF-8 incompitable bytes
                b"\x82\xa2id\xd9",
                b"\x82\xa2id\xd9",
            ),
        ],
    )
    def test_raw_message_encode_parse(self, input, should_be):
        raw_message = RawMessage.encode(
            message=input, reply_to=None, headers=None, correlation_id="id"
        )
        parsed, _ = RawMessage.parse(raw_message)
        assert parsed == should_be
