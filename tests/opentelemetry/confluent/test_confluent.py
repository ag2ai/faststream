import asyncio
from typing import Optional
from unittest.mock import Mock

import pytest
from dirty_equals import IsStr, IsUUID
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import Span, TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv.trace import SpanAttributes as SpanAttr
from opentelemetry.trace import SpanKind

from faststream.confluent import KafkaBroker
from faststream.confluent.opentelemetry import KafkaTelemetryMiddleware
from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME
from faststream.opentelemetry.middleware import MessageAction as Action
from tests.brokers.confluent.basic import ConfluentTestcaseConfig

from ..basic import LocalTelemetryTestcase


@pytest.mark.confluent()
class TestTelemetry(ConfluentTestcaseConfig, LocalTelemetryTestcase):
    messaging_system = "kafka"
    include_messages_counters = True
    broker_class = KafkaBroker
    telemetry_middleware_class = KafkaTelemetryMiddleware

    def assert_span(
        self,
        span: Span,
        action: str,
        queue: str,
        msg: str,
        parent_span_id: Optional[str] = None,
    ) -> None:
        attrs = span.attributes
        assert attrs[SpanAttr.MESSAGING_SYSTEM] == self.messaging_system
        assert attrs[SpanAttr.MESSAGING_MESSAGE_CONVERSATION_ID] == IsUUID
        assert span.name == f"{self.destination_name(queue)} {action}"
        assert span.kind in (SpanKind.CONSUMER, SpanKind.PRODUCER)

        if span.kind == SpanKind.PRODUCER and action in (Action.CREATE, Action.PUBLISH):
            assert attrs[SpanAttr.MESSAGING_DESTINATION_NAME] == queue

        if span.kind == SpanKind.CONSUMER and action in (Action.CREATE, Action.PROCESS):
            assert attrs[MESSAGING_DESTINATION_PUBLISH_NAME] == queue
            assert attrs[SpanAttr.MESSAGING_MESSAGE_ID] == IsStr(regex=r"0-.+")
            assert attrs[SpanAttr.MESSAGING_KAFKA_DESTINATION_PARTITION] == 0
            assert attrs[SpanAttr.MESSAGING_KAFKA_MESSAGE_OFFSET] == 0

        if action == Action.PROCESS:
            assert attrs[SpanAttr.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES] == len(msg)
            assert attrs[SpanAttr.MESSAGING_OPERATION] == action

        if action == Action.PUBLISH:
            assert attrs[SpanAttr.MESSAGING_OPERATION] == action

        if parent_span_id:
            assert span.parent.span_id == parent_span_id

    async def test_batch(
        self,
        event: asyncio.Event,
        queue: str,
        mock: Mock,
        meter_provider: MeterProvider,
        metric_reader: InMemoryMetricReader,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
    ):
        mid = self.telemetry_middleware_class(
            meter_provider=meter_provider, tracer_provider=tracer_provider
        )
        broker = self.broker_class(middlewares=(mid,))
        expected_msg_count = 3
        expected_link_count = 1
        expected_link_attrs = {"messaging.batch.message_count": 3}

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @broker.subscriber(*args, **kwargs)
        async def handler(m):
            mock(m)
            event.set()

        broker = self.patch_broker(broker)

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(broker.publish_batch(1, "hi", 3, topic=queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        metrics = self.get_metrics(metric_reader)
        spans = self.get_spans(trace_exporter)
        _, publish, create_process, process = spans

        assert (
            publish.attributes[SpanAttr.MESSAGING_BATCH_MESSAGE_COUNT]
            == expected_msg_count
        )
        assert (
            process.attributes[SpanAttr.MESSAGING_BATCH_MESSAGE_COUNT]
            == expected_msg_count
        )
        assert len(create_process.links) == expected_link_count
        assert create_process.links[0].attributes == expected_link_attrs
        self.assert_metrics(metrics, count=expected_msg_count)

        assert event.is_set()
        mock.assert_called_once_with([1, "hi", 3])

    async def test_batch_publish_with_single_consume(
        self,
        queue: str,
        meter_provider: MeterProvider,
        metric_reader: InMemoryMetricReader,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
    ):
        mid = self.telemetry_middleware_class(
            meter_provider=meter_provider, tracer_provider=tracer_provider
        )
        broker = self.broker_class(middlewares=(mid,))
        msgs_queue = asyncio.Queue(maxsize=3)
        expected_msg_count = 3
        expected_link_count = 1
        expected_span_count = 8
        expected_pub_batch_count = 1

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg):
            await msgs_queue.put(msg)

        broker = self.patch_broker(broker)

        async with broker:
            await broker.start()
            await broker.publish_batch(1, "hi", 3, topic=queue)
            result, _ = await asyncio.wait(
                (
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                ),
                timeout=self.timeout,
            )

        metrics = self.get_metrics(metric_reader)
        proc_dur, proc_msg, pub_dur, pub_msg = metrics
        spans = self.get_spans(trace_exporter)
        publish = spans[1]
        create_processes = [spans[2], spans[4], spans[6]]

        assert len(spans) == expected_span_count
        assert (
            publish.attributes[SpanAttr.MESSAGING_BATCH_MESSAGE_COUNT]
            == expected_msg_count
        )
        for cp in create_processes:
            assert len(cp.links) == expected_link_count

        assert proc_msg.data.data_points[0].value == expected_msg_count
        assert pub_msg.data.data_points[0].value == expected_msg_count
        assert proc_dur.data.data_points[0].count == expected_msg_count
        assert pub_dur.data.data_points[0].count == expected_pub_batch_count

        assert {1, "hi", 3} == {r.result() for r in result}

    async def test_single_publish_with_batch_consume(
        self,
        event: asyncio.Event,
        queue: str,
        mock: Mock,
        meter_provider: MeterProvider,
        metric_reader: InMemoryMetricReader,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
    ):
        mid = self.telemetry_middleware_class(
            meter_provider=meter_provider, tracer_provider=tracer_provider
        )
        broker = self.broker_class(middlewares=(mid,))
        expected_msg_count = 2
        expected_link_count = 2
        expected_span_count = 6
        expected_process_batch_count = 1

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @broker.subscriber(*args, **kwargs)
        async def handler(m):
            m.sort()
            mock(m)
            event.set()

        broker = self.patch_broker(broker)

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(broker.publish("hi", topic=queue)),
                asyncio.create_task(broker.publish("buy", topic=queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        metrics = self.get_metrics(metric_reader)
        proc_dur, proc_msg, pub_dur, pub_msg = metrics
        spans = self.get_spans(trace_exporter)
        create_process = spans[-2]

        assert len(spans) == expected_span_count
        assert len(create_process.links) == expected_link_count
        assert proc_msg.data.data_points[0].value == expected_msg_count
        assert pub_msg.data.data_points[0].value == expected_msg_count
        assert proc_dur.data.data_points[0].count == expected_process_batch_count
        assert pub_dur.data.data_points[0].count == expected_msg_count

        assert event.is_set()
        mock.assert_called_once_with(["buy", "hi"])
