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
async def test_parse_message_maps_null_value_to_tombstone() -> None:
    parsed = await AsyncConfluentParser().parse_message(_fake_message(None))

    assert parsed.body is TOMBSTONE


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_parse_message_keeps_genuine_empty_value_as_bytes() -> None:
    """An actual empty payload must stay distinguishable from a tombstone."""
    parsed = await AsyncConfluentParser().parse_message(_fake_message(b""))

    assert parsed.body == b""
