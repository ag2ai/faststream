from json import JSONDecodeError

import pytest
from aiokafka import ConsumerRecord

from faststream.kafka import TOMBSTONE, Tombstone
from faststream.kafka.message import KafkaMessage
from faststream.kafka.parser import AioKafkaBatchParser, AioKafkaParser
from tests.brokers.base.parser import CustomParserTestcase

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.connected()
class TestCustomParser(KafkaTestcaseConfig, CustomParserTestcase):
    pass


def _record(value: bytes | None, content_type: str = "") -> ConsumerRecord:
    return ConsumerRecord(
        topic="test",
        partition=0,
        offset=0,
        timestamp=0,
        timestamp_type=0,
        key=b"k",
        value=value,
        checksum=0,
        serialized_key_size=1,
        serialized_value_size=0,
        headers=(("content-type", content_type.encode()),) if content_type else (),
    )


@pytest.mark.asyncio()
@pytest.mark.kafka()
@pytest.mark.parametrize(
    ("value", "tombstone"),
    ((None, True), (b"", False), (b"{}", False)),
)
async def test_parse_message_flags_only_a_null_value(
    value: bytes | None,
    tombstone: bool,
) -> None:
    parser = AioKafkaParser(msg_class=KafkaMessage, regex=None)
    parsed = await parser.parse_message(_record(value))

    assert parsed.no_body is tombstone
    assert parsed.tombstone is tombstone
    assert parsed.body == (value or b"")
    assert isinstance(parsed.body, Tombstone) is tombstone


@pytest.mark.asyncio()
@pytest.mark.kafka()
async def test_parse_batch_marks_the_null_records_one_by_one() -> None:
    parser = AioKafkaBatchParser(msg_class=KafkaMessage, regex=None)

    parsed = await parser.parse_batch((_record(b"{}"), _record(None), _record(b"")))

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
@pytest.mark.kafka()
async def test_decode_keeps_a_tombstone_empty_whatever_the_content_type() -> None:
    parser = AioKafkaParser(msg_class=KafkaMessage, regex=None)

    tombstone = await parser.parse_message(_record(None, "application/json"))
    assert await parser.decode_message(tombstone) is TOMBSTONE

    # a genuinely empty body with the same content-type is not a tombstone
    # and still fails to decode, exactly as it does without this feature.
    empty = await parser.parse_message(_record(b"", "application/json"))
    with pytest.raises(JSONDecodeError):
        await parser.decode_message(empty)


@pytest.mark.asyncio()
@pytest.mark.kafka()
async def test_decode_batch_keeps_only_the_tombstone_empty() -> None:
    parser = AioKafkaBatchParser(msg_class=KafkaMessage, regex=None)
    records = (
        _record(b'{"x": 1}', "application/json"),
        _record(None, "application/json"),
    )

    decoded = await parser.decode_batch(await parser.parse_batch(records))

    assert decoded == [{"x": 1}, b""]
