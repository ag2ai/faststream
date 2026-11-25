import pytest
from aiokafka import ConsumerRecord
from opentelemetry.sdk.trace import SpanLimits, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind

from faststream._internal.context.repository import ContextRepo
from faststream.confluent.opentelemetry import (
    KafkaTelemetryMiddleware as ConfluentTelemetryMiddleware,
)
from faststream.confluent.testing import MockConfluentMessage
from faststream.kafka.opentelemetry import KafkaTelemetryMiddleware
from faststream.message import StreamMessage
from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME


async def process_message(message: StreamMessage[object]) -> None:
    pass


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "backend",
    (
        pytest.param("aiokafka", marks=pytest.mark.kafka()),
        pytest.param("confluent", marks=pytest.mark.confluent()),
    ),
)
@pytest.mark.parametrize(
    ("topics", "max_attributes", "expected_topic"),
    (
        pytest.param(("orders", "orders"), 2, None, id="two-attribute-slots"),
        pytest.param(("orders", "orders"), None, "orders", id="same-topic"),
        pytest.param(("orders", "payments", "orders"), None, None, id="mixed-topics"),
        pytest.param(("orders",), None, "orders", id="singleton"),
    ),
)
async def test_batch_consume_destination(
    backend: str,
    topics: tuple[str, ...],
    max_attributes: int | None,
    expected_topic: str | None,
) -> None:
    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider(
        span_limits=SpanLimits(max_span_attributes=max_attributes),
    )
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))

    records: tuple[ConsumerRecord, ...] | tuple[MockConfluentMessage, ...]
    middleware: KafkaTelemetryMiddleware | ConfluentTelemetryMiddleware
    if backend == "aiokafka":
        middleware = KafkaTelemetryMiddleware(tracer_provider=tracer_provider)
        records = tuple(
            ConsumerRecord(
                topic=topic,
                partition=partition,
                offset=0,
                timestamp=0,
                timestamp_type=0,
                key=None,
                value=b"x",
                checksum=None,
                serialized_key_size=-1,
                serialized_value_size=1,
                headers=(),
            )
            for partition, topic in enumerate(topics)
        )
    else:
        middleware = ConfluentTelemetryMiddleware(tracer_provider=tracer_provider)
        records = tuple(
            MockConfluentMessage(
                raw_msg=b"x",
                topic=topic,
                key=b"",
                headers=(),
                offset=0,
                partition=partition,
                timestamp_type=0,
                timestamp_ms=0,
            )
            for partition, topic in enumerate(topics)
        )

    message: StreamMessage[object] = StreamMessage(
        raw_message=records,
        body=[b"x"] * len(records),
        batch_headers=[{} for _ in records],
    )
    scope = middleware(records, context=ContextRepo())
    try:
        async with scope:
            await scope.consume_scope(process_message, message)

        (span,) = (
            span
            for span in exporter.get_finished_spans()
            if (span.attributes or {}).get(SpanAttributes.MESSAGING_OPERATION)
            == "process"
        )
        assert span.kind is SpanKind.CONSUMER
        attrs = span.attributes or {}
        assert attrs[MESSAGING_DESTINATION_PUBLISH_NAME] == "orders"
        if expected_topic is None:
            assert SpanAttributes.MESSAGING_DESTINATION_NAME not in attrs
        else:
            assert attrs[SpanAttributes.MESSAGING_DESTINATION_NAME] == expected_topic
    finally:
        tracer_provider.shutdown()
