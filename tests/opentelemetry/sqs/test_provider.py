from types import SimpleNamespace

import pytest
from opentelemetry.semconv.trace import SpanAttributes as SpanAttr

from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME
from faststream.sqs.opentelemetry.provider import (
    BatchSQSTelemetrySettingsProvider,
    SQSTelemetrySettingsProvider,
    telemetry_attributes_provider_factory,
)


@pytest.mark.sqs()
def test_single_consume_attrs() -> None:
    body = b"Hello"
    queue_url = "http://localhost:9324/000000000000/queue"
    message = SimpleNamespace(
        body=body,
        queue_url=queue_url,
        message_id="1",
        correlation_id="1",
    )

    attrs = SQSTelemetrySettingsProvider().get_consume_attrs_from_message(message)

    assert attrs[SpanAttr.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES] == len(body)
    assert attrs[MESSAGING_DESTINATION_PUBLISH_NAME] == queue_url


@pytest.mark.sqs()
def test_batch_consume_attrs() -> None:
    bodies = [b"Hi ", b"again, ", b"FastStream!"]
    queue_url = "http://localhost:9324/000000000000/queue"
    message = SimpleNamespace(
        body=bodies,
        queue_url=queue_url,
        message_id="1",
        correlation_id="1",
        raw_message=[SimpleNamespace() for _ in bodies],
    )

    attrs = BatchSQSTelemetrySettingsProvider().get_consume_attrs_from_message(message)

    assert attrs[SpanAttr.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES] == len(
        bytearray().join(bodies),
    )
    assert attrs[SpanAttr.MESSAGING_BATCH_MESSAGE_COUNT] == len(bodies)
    assert attrs[MESSAGING_DESTINATION_PUBLISH_NAME] == queue_url


@pytest.mark.sqs()
@pytest.mark.parametrize(
    ("msg", "expected_provider"),
    (
        pytest.param(
            [{"Body": "1"}, {"Body": "2"}],
            BatchSQSTelemetrySettingsProvider(),
            id="batch message",
        ),
        pytest.param(
            {"Body": "1"},
            SQSTelemetrySettingsProvider(),
            id="single message",
        ),
        pytest.param(
            None,
            SQSTelemetrySettingsProvider(),
            id="None message",
        ),
    ),
)
def test_telemetry_provider_factory(msg, expected_provider) -> None:
    provider = telemetry_attributes_provider_factory(msg)

    assert isinstance(provider, type(expected_provider))
