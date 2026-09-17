from json import JSONDecodeError
from unittest.mock import MagicMock

import pytest

from faststream.confluent import TOMBSTONE, Tombstone
from faststream.confluent.parser import AsyncConfluentParser
from tests.brokers.base.parser import CustomParserTestcase

from .basic import ConfluentTestcaseConfig


@pytest.mark.connected()
@pytest.mark.confluent()
class TestCustomParser(ConfluentTestcaseConfig, CustomParserTestcase):
    pass


def _fake_message(value: bytes | None, content_type: str = "") -> MagicMock:
    message = MagicMock()
    message.value.return_value = value
    message.headers.return_value = (
        [("content-type", content_type)] if content_type else None
    )
    message.offset.return_value = 0
    message.timestamp.return_value = (0, 0)
    return message


@pytest.mark.asyncio()
@pytest.mark.confluent()
@pytest.mark.parametrize(
    ("value", "tombstone"),
    ((None, True), (b"", False), (b"{}", False)),
)
async def test_parse_message_flags_only_a_null_value(
    value: bytes | None,
    tombstone: bool,
) -> None:
    parsed = await AsyncConfluentParser().parse_message(_fake_message(value))

    assert parsed.no_body is tombstone
    assert parsed.tombstone is tombstone
    assert parsed.body == (value or b"")
    assert isinstance(parsed.body, Tombstone) is tombstone


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_parse_batch_marks_the_null_records_one_by_one() -> None:
    parser = AsyncConfluentParser()

    parsed = await parser.parse_batch(
        (_fake_message(b"{}"), _fake_message(None), _fake_message(b"")),
    )

    assert parsed.body == [b"{}", b"", b""]
    assert [isinstance(body, Tombstone) for body in parsed.body] == [
        False,
        True,
        False,
    ]
    # a batch answers per record, so the message-level flag stays False
    assert parsed.no_body is False
    assert parsed.tombstone is False


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_decode_keeps_a_tombstone_empty_whatever_the_content_type() -> None:
    parser = AsyncConfluentParser()

    tombstone = await parser.parse_message(_fake_message(None, "application/json"))
    assert await parser.decode_message(tombstone) is TOMBSTONE

    # a genuinely empty body with the same content-type is not a tombstone
    # and still fails to decode, exactly as it does without this feature.
    empty = await parser.parse_message(_fake_message(b"", "application/json"))
    with pytest.raises(JSONDecodeError):
        await parser.decode_message(empty)


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_decode_batch_keeps_only_the_tombstone_empty() -> None:
    parser = AsyncConfluentParser()
    messages = (
        _fake_message(b'{"x": 1}', "application/json"),
        _fake_message(None, "application/json"),
    )

    decoded = await parser.decode_batch(await parser.parse_batch(messages))

    assert decoded == [{"x": 1}, b""]
