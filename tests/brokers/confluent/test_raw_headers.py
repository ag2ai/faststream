import pytest
from confluent_kafka import Message

from faststream.confluent.message import FAKE_CONSUMER, KafkaMessage
from faststream.confluent.testing import MockConfluentMessage

pytestmark = pytest.mark.confluent()

EXPECTED_HEADERS = {
    "utf8": "привет".encode(),
    "binary": b"\xff\x00",
    "null": None,
    "empty": b"",
    "duplicate": b"last",
    "string": "строка".encode(),
}


@pytest.fixture()
def record() -> MockConfluentMessage:
    return MockConfluentMessage(
        raw_msg=b"body",
        topic="test-topic",
        key=b"",
        headers=[
            ("utf8", "привет".encode()),
            ("binary", b"\xff\x00"),
            ("null", None),
            ("empty", b""),
            ("duplicate", b"first"),
            ("duplicate", b"last"),
            ("string", "строка"),
        ],
        offset=0,
        partition=0,
        timestamp_type=1,
        timestamp_ms=0,
    )


def test_raw_headers_preserve_wire_values(record: MockConfluentMessage) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2214."""
    message = KafkaMessage(
        raw_message=record,
        body=b"body",
        consumer=FAKE_CONSUMER,
    )
    assert (message.raw_headers, message.batch_raw_headers) == (
        EXPECTED_HEADERS,
        [],
    )


def test_batch_raw_headers_preserve_wire_values(record: MockConfluentMessage) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2214."""
    message = KafkaMessage(
        raw_message=(record, Message()),
        body=b"body",
        consumer=FAKE_CONSUMER,
    )

    assert (message.raw_headers, message.batch_raw_headers) == (
        EXPECTED_HEADERS,
        [EXPECTED_HEADERS, {}],
    )


@pytest.mark.parametrize("raw_message", (Message(), ()))
def test_missing_headers_are_empty(raw_message: Message | tuple[Message, ...]) -> None:
    message = KafkaMessage(
        raw_message=raw_message,
        body=b"",
        consumer=FAKE_CONSUMER,
    )

    assert (message.raw_headers, message.batch_raw_headers) == ({}, [])
