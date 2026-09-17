from types import SimpleNamespace

import pytest
from opentelemetry.semconv.trace import SpanAttributes

from faststream._internal.kafka import TOMBSTONE
from faststream.confluent.opentelemetry.provider import (
    BatchConfluentTelemetrySettingsProvider,
    ConfluentTelemetrySettingsProvider,
)

PAYLOAD_SIZE = SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES


def _record(queue: str) -> SimpleNamespace:
    return SimpleNamespace(
        topic=lambda: queue, partition=lambda: 0, offset=lambda: 0, key=lambda: b"k"
    )


@pytest.mark.confluent()
def test_get_consume_attrs_from_a_tombstone(queue: str) -> None:
    message = SimpleNamespace(
        body=TOMBSTONE,
        message_id="1",
        correlation_id="1",
        raw_message=_record(queue),
    )

    attrs = ConfluentTelemetrySettingsProvider().get_consume_attrs_from_message(message)

    assert attrs[PAYLOAD_SIZE] == 0


@pytest.mark.confluent()
def test_get_consume_attrs_from_a_batch_holding_a_tombstone(queue: str) -> None:
    body = [b"Hi", TOMBSTONE]
    message = SimpleNamespace(
        body=body,
        message_id="1",
        correlation_id="1",
        raw_message=[_record(queue) for _ in body],
    )

    provider = BatchConfluentTelemetrySettingsProvider()
    attrs = provider.get_consume_attrs_from_message(message)

    assert attrs[PAYLOAD_SIZE] == 2
    assert attrs[SpanAttributes.MESSAGING_BATCH_MESSAGE_COUNT] == 2
