import asyncio
from typing import Any
from unittest.mock import Mock

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv.trace import SpanAttributes as SpanAttr

from faststream.nats import JStream, NatsBroker, PullSub
from faststream.nats.opentelemetry import NatsTelemetryMiddleware
from tests.brokers.nats.test_consume import TestConsume
from tests.brokers.nats.test_publish import TestPublish
from tests.opentelemetry.basic import LocalTelemetryTestcase


@pytest.fixture()
def stream(queue):
    return JStream(queue)


@pytest.mark.nats()
class TestTelemetry(LocalTelemetryTestcase):
    messaging_system = "nats"
    include_messages_counters = True
    telemetry_middleware_class = NatsTelemetryMiddleware

    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> NatsBroker:
        return NatsBroker(apply_types=apply_types, **kwargs)

    async def test_batch(
        self,
        queue: str,
        mock: Mock,
        stream: JStream,
        meter_provider: MeterProvider,
        metric_reader: InMemoryMetricReader,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
    ) -> None:
        event = asyncio.Event()

        mid = self.telemetry_middleware_class(
            meter_provider=meter_provider,
            tracer_provider=tracer_provider,
        )
        broker = self.get_broker(middlewares=(mid,))
        expected_msg_count = 1
        expected_span_count = 4
        expected_proc_batch_count = 1

        args, kwargs = self.get_subscriber_params(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True, timeout=30.0),
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(m) -> None:
            mock(m)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            tasks = (
                asyncio.create_task(br.publish("hi", queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        metrics = self.get_metrics(metric_reader)
        proc_dur, proc_msg, pub_dur, pub_msg = metrics
        spans = self.get_spans(trace_exporter)
        process = spans[-1]
        create_batch = spans[-2]

        assert len(create_batch.links) == expected_msg_count
        assert len(spans) == expected_span_count
        assert (
            process.attributes[SpanAttr.MESSAGING_BATCH_MESSAGE_COUNT]
            == expected_msg_count
        )
        assert proc_msg.data.data_points[0].value == expected_msg_count
        assert pub_msg.data.data_points[0].value == expected_msg_count
        assert proc_dur.data.data_points[0].count == expected_proc_batch_count
        assert pub_dur.data.data_points[0].count == expected_msg_count

        assert event.is_set()
        mock.assert_called_once_with(["hi"])


@pytest.mark.nats()
class TestPublishWithTelemetry(TestPublish):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> NatsBroker:
        return NatsBroker(
            middlewares=(NatsTelemetryMiddleware(),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.nats()
class TestConsumeWithTelemetry(TestConsume):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> NatsBroker:
        return NatsBroker(
            middlewares=(NatsTelemetryMiddleware(),),
            apply_types=apply_types,
            **kwargs,
        )
