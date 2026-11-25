import asyncio
from typing import Any
from unittest.mock import MagicMock, call

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv.trace import SpanAttributes as SpanAttr

from faststream.nats import JStream, NatsBroker, PullSub
from faststream.nats.opentelemetry import NatsTelemetryMiddleware
from faststream.opentelemetry import CurrentBaggage
from tests.brokers.nats.basic import NatsTestcaseConfig
from tests.brokers.nats.test_consume import TestConsume as ConsumeCase
from tests.brokers.nats.test_publish import TestPublish as PublishCase
from tests.opentelemetry.basic import (
    LocalTelemetryTestcase,
    counter_point,
    histogram_point,
)


@pytest.fixture()
def stream(queue: str) -> Any:
    return JStream(queue)


@pytest.mark.connected()
@pytest.mark.nats()
class TestTelemetry(NatsTestcaseConfig, LocalTelemetryTestcase):
    messaging_system = "nats"
    include_messages_counters = True
    telemetry_middleware_class = NatsTelemetryMiddleware

    async def test_batch(
        self,
        queue: str,
        mock: MagicMock,
        meter_provider: MeterProvider,
        metric_reader: InMemoryMetricReader,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
        event: asyncio.Event,
    ) -> None:
        mid = self.telemetry_middleware_class(
            meter_provider=meter_provider,
            tracer_provider=tracer_provider,
        )
        broker = self.get_broker(middlewares=(mid,), apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            stream=JStream(queue, allow_atomic=True),
            pull_sub=PullSub(3, batch=True, timeout=None),
        )

        expected_msg_count = 3
        expected_link_count = 1
        expected_link_attrs = {SpanAttr.MESSAGING_BATCH_MESSAGE_COUNT: expected_msg_count}
        expected_baggage = {"with_batch": "True"}

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(m: list[str], baggage: CurrentBaggage) -> None:
            mock(m, baggage=baggage.get_all(), batch=baggage.get_all_batch())
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(
                        br.publish_batch(
                            "first", "second", "third", subject=queue, stream=queue
                        )
                    ),
                ),
                timeout=self.timeout,
            )

        _, publish, create_process, process = self.get_spans(trace_exporter)

        assert (publish.attributes or {})[
            SpanAttr.MESSAGING_BATCH_MESSAGE_COUNT
        ] == expected_msg_count
        assert (process.attributes or {})[
            SpanAttr.MESSAGING_BATCH_MESSAGE_COUNT
        ] == expected_msg_count
        assert len(create_process.links) == expected_link_count
        assert create_process.links[0].attributes == expected_link_attrs

        self.assert_metrics(self.get_metrics(metric_reader), count=expected_msg_count)
        mock.assert_called_once_with(
            ["first", "second", "third"],
            baggage=expected_baggage,
            batch=[expected_baggage] * 3,
        )

    async def test_batch_publish_with_single_consume(
        self,
        queue: str,
        mock: MagicMock,
        meter_provider: MeterProvider,
        metric_reader: InMemoryMetricReader,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
        event: asyncio.Event,
    ) -> None:
        mid = self.telemetry_middleware_class(
            meter_provider=meter_provider,
            tracer_provider=tracer_provider,
        )
        broker = self.get_broker(middlewares=(mid,), apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            stream=JStream(queue, allow_atomic=True),
        )

        expected_msg_count = 3
        expected_span_count = 8
        expected_link_count = 1
        expected_pub_batch_count = 1
        expected_baggage = {"with_batch": "True"}

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(m: str, baggage: CurrentBaggage) -> None:
            mock(m, baggage=baggage.get_all(), batch=baggage.get_all_batch())
            if mock.call_count == 3:
                event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(
                        br.publish_batch(
                            "first", "second", "third", subject=queue, stream=queue
                        )
                    ),
                ),
                timeout=self.timeout,
            )

        metrics = self.get_metrics(metric_reader)
        proc_dur, proc_msg, pub_dur, pub_msg = metrics

        spans = self.get_spans(trace_exporter)
        _, publish, create_first, _, create_second, _, create_third, _ = spans

        assert len(spans) == expected_span_count
        assert (publish.attributes or {})[
            SpanAttr.MESSAGING_BATCH_MESSAGE_COUNT
        ] == expected_msg_count
        for span in (create_first, create_second, create_third):
            assert len(span.links) == expected_link_count

        assert counter_point(pub_msg).value == expected_msg_count
        assert counter_point(proc_msg).value == expected_msg_count
        assert histogram_point(proc_dur).count == expected_msg_count
        assert histogram_point(pub_dur).count == expected_pub_batch_count

        assert mock.call_args_list == [
            call("first", baggage=expected_baggage, batch=[]),
            call("second", baggage=expected_baggage, batch=[]),
            call("third", baggage=expected_baggage, batch=[]),
        ]

    async def test_single_publish_with_batch_consume(
        self,
        queue: str,
        mock: MagicMock,
        stream: JStream,
        meter_provider: MeterProvider,
        metric_reader: InMemoryMetricReader,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
        event: asyncio.Event,
    ) -> None:
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

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(m: Any) -> None:
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
        assert (process.attributes or {})[
            SpanAttr.MESSAGING_BATCH_MESSAGE_COUNT
        ] == expected_msg_count
        assert counter_point(proc_msg).value == expected_msg_count
        assert counter_point(pub_msg).value == expected_msg_count
        assert histogram_point(proc_dur).count == expected_proc_batch_count
        assert histogram_point(pub_dur).count == expected_msg_count

        assert event.is_set()
        mock.assert_called_once_with(["hi"])


@pytest.mark.connected()
@pytest.mark.nats()
class TestPublishWithTelemetry(PublishCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> NatsBroker:
        return NatsBroker(
            middlewares=(NatsTelemetryMiddleware(),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.connected()
@pytest.mark.nats()
class TestConsumeWithTelemetry(ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> NatsBroker:
        return NatsBroker(
            middlewares=(NatsTelemetryMiddleware(),),
            apply_types=apply_types,
            **kwargs,
        )
