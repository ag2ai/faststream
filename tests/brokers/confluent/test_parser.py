from unittest.mock import MagicMock

import pytest

from faststream.confluent.parser import AsyncConfluentParser
from faststream.message import TOMBSTONE
from tests.brokers.base.parser import CustomParserTestcase

from .basic import ConfluentTestcaseConfig


@pytest.mark.connected()
@pytest.mark.confluent()
class TestCustomParser(ConfluentTestcaseConfig, CustomParserTestcase):
    pass


def _fake_message(value: bytes | None) -> MagicMock:
    message = MagicMock()
    message.value.return_value = value
    message.headers.return_value = None
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

    assert parsed.tombstone is tombstone
    if tombstone:
        assert parsed.body is TOMBSTONE
    else:
        assert parsed.body == value


@pytest.mark.asyncio()
async def test_parse_batch_flags_a_batch_holding_a_tombstone() -> None:
    parser = AsyncConfluentParser()

    mixed = await parser.parse_batch((_fake_message(b"{}"), _fake_message(None)))
    plain = await parser.parse_batch((_fake_message(b"{}"), _fake_message(b"")))

    assert mixed.tombstone is True
    assert plain.tombstone is False
