import pytest
from aiokafka import ConsumerRecord

from faststream.kafka import Tombstone
from faststream.kafka.message import KafkaMessage
from faststream.kafka.parser import AioKafkaBatchParser, AioKafkaParser
from tests.brokers.base.parser import CustomParserTestcase, KafkaTombstoneParserTestcase

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


@pytest.mark.kafka()
class TestTombstoneParser(KafkaTombstoneParserTestcase):
    make_record = staticmethod(_record)

    def make_parser(self) -> AioKafkaBatchParser:
        return AioKafkaBatchParser(msg_class=KafkaMessage, regex=None)
