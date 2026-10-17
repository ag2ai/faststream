from dataclasses import replace
from typing import Any

import pytest
from aiokafka import ConsumerRecord

from faststream.kafka.message import FAKE_CONSUMER, KafkaMessage

pytestmark = pytest.mark.kafka()


@pytest.mark.parametrize("batch", (False, True))
def test_raw_headers_preserve_wire_values(*, batch: bool) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2214."""
    # aiokafka's annotations omit Kafka's nullable header values.
    headers: Any = [
        ("utf8", "привет".encode()),
        ("binary", b"\xff\x00"),
        ("null", None),
        ("empty", b""),
        ("duplicate", b"first"),
        ("duplicate", b"last"),
    ]
    record: ConsumerRecord[bytes, bytes] = ConsumerRecord(
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
    second = replace(record, headers=[])
    message = KafkaMessage(
        raw_message=(record, second) if batch else record,
        body=b"body",
        consumer=FAKE_CONSUMER,
    )
    expected = {
        "utf8": "привет".encode(),
        "binary": b"\xff\x00",
        "null": None,
        "empty": b"",
        "duplicate": b"last",
    }
    assert (message.raw_headers, message.batch_raw_headers) == (
        expected,
        [expected, {}] if batch else [],
    )


def test_empty_batch_has_no_raw_headers() -> None:
    message = KafkaMessage(raw_message=(), body=[], consumer=FAKE_CONSUMER)

    assert (message.raw_headers, message.batch_raw_headers) == ({}, [])
