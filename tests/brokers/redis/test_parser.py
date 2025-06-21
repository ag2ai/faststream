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
    def test_raw_message_encode_parse(self, input, should_be) -> None:
        raw_message = RawMessage.encode(
            message=input, reply_to=None, headers=None, correlation_id="id"
        )
        parsed, _ = RawMessage.parse(raw_message)
        assert parsed == should_be
