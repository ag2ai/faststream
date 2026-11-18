from dataclasses import replace
from typing import Any

import pytest
from aiokafka import ConsumerRecord

from faststream.kafka.message import FAKE_CONSUMER, KafkaMessage

pytestmark = pytest.mark.kafka()

EXPECTED_HEADERS = {
    "utf8": "привет".encode(),
    "binary": b"\xff\x00",
    "null": None,
    "empty": b"",
    "duplicate": b"last",
}


@pytest.fixture()
def record() -> ConsumerRecord[bytes, bytes]:
    # aiokafka's annotations omit Kafka's nullable header values.
    headers: Any = [
        ("utf8", "привет".encode()),
        ("binary", b"\xff\x00"),
        ("null", None),
        ("empty", b""),
        ("duplicate", b"first"),
        ("duplicate", b"last"),
    ]
    return ConsumerRecord(
        topic="test-topic",
        partition=0,
        offset=0,
        timestamp=0,
        timestamp_type=1,
        key=None,
        value=b"body",
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=4,
        headers=headers,
    )


def test_raw_headers_preserve_wire_values(record: ConsumerRecord[bytes, bytes]) -> None:
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


def test_batch_raw_headers_preserve_wire_values(
    record: ConsumerRecord[bytes, bytes],
) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2214."""
    message = KafkaMessage(
        raw_message=(record, replace(record, headers=[])),
        body=b"body",
        consumer=FAKE_CONSUMER,
    )

    assert (message.raw_headers, message.batch_raw_headers) == (
        EXPECTED_HEADERS,
        [EXPECTED_HEADERS, {}],
    )


def test_empty_batch_has_no_raw_headers() -> None:
    message = KafkaMessage(raw_message=(), body=[], consumer=FAKE_CONSUMER)

    assert (message.raw_headers, message.batch_raw_headers) == ({}, [])
