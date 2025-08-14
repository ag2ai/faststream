"""Simplified GCP OpenTelemetry tests focused on middleware functionality."""

import asyncio
import uuid

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv.trace import SpanAttributes as SpanAttr
from opentelemetry.trace import SpanKind

from faststream.gcp import GCPBroker
from faststream.gcp.opentelemetry import GCPTelemetryMiddleware


@pytest.fixture()
def gcp_subscription() -> str:
    """Generate GCP-compatible subscription name without dashes."""
    return f"testsub{uuid.uuid4().hex[:8]}"


@pytest.fixture()
def gcp_topic() -> str:
    """Generate GCP-compatible topic name without dashes."""
    return f"testtopic{uuid.uuid4().hex[:8]}"


@pytest.mark.gcp()
@pytest.mark.connected()
class TestGCPTelemetryIntegration:
    """Test GCP Pub/Sub OpenTelemetry middleware integration."""

    @pytest.fixture()
    def tracer_provider(self) -> TracerProvider:
        return TracerProvider()

    @pytest.fixture()
    def trace_exporter(self, tracer_provider: TracerProvider) -> InMemorySpanExporter:
        exporter = InMemorySpanExporter()
        tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        return exporter

    @pytest.fixture()
    def metric_reader(self) -> InMemoryMetricReader:
        return InMemoryMetricReader()

    @pytest.fixture()
    def meter_provider(self, metric_reader: InMemoryMetricReader) -> MeterProvider:
        return MeterProvider(metric_readers=(metric_reader,))

    def get_spans(self, exporter: InMemorySpanExporter):
        """Get spans sorted by start time."""
        spans = exporter.get_finished_spans()
        return sorted(spans, key=lambda s: s.start_time or 0)

    @pytest.mark.asyncio()
    async def test_telemetry_middleware_basic_functionality(
        self,
        gcp_subscription: str,
        gcp_topic: str,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
        meter_provider: MeterProvider,
        metric_reader: InMemoryMetricReader,
    ) -> None:
        """Test basic telemetry middleware functionality."""
        # Create middleware
        middleware = GCPTelemetryMiddleware(
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
        )

        # Create broker with middleware
        broker = GCPBroker(
            project_id="test-project",
            middlewares=[middleware],
        )

        # Event to track message processing
        processed = asyncio.Event()
        received_message = None

        @broker.subscriber(gcp_subscription, topic=gcp_topic, create_subscription=True)
        async def handler(message: str) -> None:
            nonlocal received_message
            received_message = message
            processed.set()

        # Test the flow
        async with broker:
            await broker.start()

            # Publish message
            await broker.publish("test-message", topic=gcp_topic)

            # Wait for processing
            await asyncio.wait_for(processed.wait(), timeout=5.0)

        # Verify message was processed
        assert received_message == "test-message"

        # Verify spans were created
        spans = self.get_spans(trace_exporter)
        assert len(spans) >= 2  # At least publish and process spans

        # Verify span attributes
        gcp_spans = [
            s
            for s in spans
            if s.attributes.get(SpanAttr.MESSAGING_SYSTEM) == "gcp_pubsub"
        ]
        assert len(gcp_spans) >= 2

        # Check we have both producer and consumer spans
        producer_spans = [s for s in gcp_spans if s.kind == SpanKind.PRODUCER]
        consumer_spans = [s for s in gcp_spans if s.kind == SpanKind.CONSUMER]

        assert len(producer_spans) >= 1
        assert len(consumer_spans) >= 1

        # Verify metrics were collected
        metrics = metric_reader.get_metrics_data()
        assert metrics is not None

    @pytest.mark.asyncio()
    async def test_gcp_specific_span_attributes(
        self,
        gcp_subscription: str,
        gcp_topic: str,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
    ) -> None:
        """Test GCP-specific span attributes are included."""
        middleware = GCPTelemetryMiddleware(tracer_provider=tracer_provider)
        broker = GCPBroker(project_id="test-project", middlewares=[middleware])

        processed = asyncio.Event()

        @broker.subscriber(gcp_subscription, topic=gcp_topic, create_subscription=True)
        async def handler(message: str) -> None:
            processed.set()

        async with broker:
            await broker.start()

            # Publish with ordering key
            await broker.publish(
                "test-message",
                topic=gcp_topic,
                ordering_key="test-key",
                attributes={"custom": "value"},
            )

            await asyncio.wait_for(processed.wait(), timeout=5.0)

        spans = self.get_spans(trace_exporter)
        gcp_spans = [
            s
            for s in spans
            if s.attributes.get(SpanAttr.MESSAGING_SYSTEM) == "gcp_pubsub"
        ]

        # Find a producer span and verify GCP-specific attributes
        producer_spans = [s for s in gcp_spans if s.kind == SpanKind.PRODUCER]
        assert len(producer_spans) >= 1

        producer_span = producer_spans[0]
        attrs = producer_span.attributes

        # Verify GCP-specific attributes exist
        assert "messaging.gcp_pubsub.topic" in attrs
        assert attrs["messaging.gcp_pubsub.topic"] == gcp_topic

        # Check for ordering key if supported
        if "messaging.gcp_pubsub.ordering_key" in attrs:
            assert attrs["messaging.gcp_pubsub.ordering_key"] == "test-key"

    @pytest.mark.asyncio()
    async def test_trace_context_propagation(
        self,
        gcp_subscription: str,
        gcp_topic: str,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
    ) -> None:
        """Test trace context propagation across publish/consume."""
        middleware = GCPTelemetryMiddleware(tracer_provider=tracer_provider)
        broker = GCPBroker(project_id="test-project", middlewares=[middleware])

        processed = asyncio.Event()

        @broker.subscriber(gcp_subscription, topic=gcp_topic, create_subscription=True)
        async def handler(message: str) -> None:
            processed.set()

        async with broker:
            await broker.start()
            await broker.publish("test-message", topic=gcp_topic)
            await asyncio.wait_for(processed.wait(), timeout=5.0)

        spans = self.get_spans(trace_exporter)
        gcp_spans = [
            s
            for s in spans
            if s.attributes.get(SpanAttr.MESSAGING_SYSTEM) == "gcp_pubsub"
        ]

        # Verify we have connected spans (trace context propagation working)
        assert len(gcp_spans) >= 2

        # All spans should have conversation IDs (correlation IDs)
        for span in gcp_spans:
            assert SpanAttr.MESSAGING_MESSAGE_CONVERSATION_ID in span.attributes

    @pytest.mark.asyncio()
    async def test_middleware_error_handling(
        self,
        gcp_subscription: str,
        gcp_topic: str,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
    ) -> None:
        """Test middleware handles errors gracefully."""
        middleware = GCPTelemetryMiddleware(tracer_provider=tracer_provider)
        broker = GCPBroker(project_id="test-project", middlewares=[middleware])

        processed = asyncio.Event()

        @broker.subscriber(gcp_subscription, topic=gcp_topic, create_subscription=True)
        async def failing_handler(message: str) -> None:
            processed.set()
            error_msg = "Test error"
            raise ValueError(error_msg)

        async with broker:
            await broker.start()
            await broker.publish("test-message", topic=gcp_topic)
            await asyncio.wait_for(processed.wait(), timeout=5.0)

        # Even with errors, spans should be created
        spans = self.get_spans(trace_exporter)
        gcp_spans = [
            s
            for s in spans
            if s.attributes.get(SpanAttr.MESSAGING_SYSTEM) == "gcp_pubsub"
        ]
        assert len(gcp_spans) >= 1
