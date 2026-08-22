from unittest.mock import MagicMock

import pytest

from faststream.confluent.parser import AsyncConfluentParser
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
    assert parsed.body == (value or b"")
