import asyncio
from typing import Any
from unittest.mock import Mock

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv.trace import SpanAttributes as SpanAttr

from faststream.opentelemetry import Baggage, CurrentBaggage
from faststream.redis import ListSub, RedisBroker
from faststream.redis.opentelemetry import RedisTelemetryMiddleware
from tests.brokers.redis.test_consume import (
    TestConsume,
    TestConsumeList,
    TestConsumeStream,
)
from tests.brokers.redis.test_publish import TestPublish
from tests.opentelemetry.basic import LocalTelemetryTestcase


@pytest.mark.redis()
class TestTelemetry(LocalTelemetryTestcase):
    messaging_system = "redis"
    include_messages_counters = True
    telemetry_middleware_class = RedisTelemetryMiddleware

    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> RedisBroker:
        return RedisBroker(apply_types=apply_types, **kwargs)

    async def test_batch(
        self,
        queue: str,
        mock: Mock,
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
        broker = self.get_broker(middlewares=(mid,), apply_types=True)
        expected_msg_count = 3
        expected_link_count = 1
        expected_link_attrs = {"messaging.batch.message_count": 3}
        expected_baggage = {"with_batch": "True"}
        expected_baggage_batch = [{"with_batch": "True"}] * 3

        args, kwargs = self.get_subscriber_params(list=ListSub(queue, batch=True))

        @broker.subscriber(*args, **kwargs)
        async def handler(m, baggage: CurrentBaggage) -> None:
            assert baggage.get_all() == expected_baggage
            assert baggage.get_all_batch() == expected_baggage_batch
            mock(m)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            tasks = (
                asyncio.create_task(br.publish_batch(1, "hi", 3, list=queue)),
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
    ) -> None:
        mid = self.telemetry_middleware_class(
            meter_provider=meter_provider,
            tracer_provider=tracer_provider,
        )
        broker = self.get_broker(middlewares=(mid,), apply_types=True)
        msgs_queue = asyncio.Queue(maxsize=3)
        expected_msg_count = 3
        expected_link_count = 1
        expected_span_count = 8
        expected_pub_batch_count = 1
        expected_baggage = {"with_batch": "True"}
        expected_baggage_batch = []

        args, kwargs = self.get_subscriber_params(list=ListSub(queue))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg, baggage: CurrentBaggage) -> None:
            assert baggage.get_all() == expected_baggage
            assert baggage.get_all_batch() == expected_baggage_batch
            await msgs_queue.put(msg)

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish_batch(1, "hi", 3, list=queue)
            result, _ = await asyncio.wait(
                (
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                ),
                timeout=3,
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
        queue: str,
        mock: Mock,
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
        broker = self.get_broker(middlewares=(mid,), apply_types=True)
        expected_msg_count = 2
        expected_link_count = 2
        expected_span_count = 6
        expected_process_batch_count = 1
        expected_baggage = {"foo": "bar", "bar": "baz"}

        args, kwargs = self.get_subscriber_params(list=ListSub(queue, batch=True))

        @broker.subscriber(*args, **kwargs)
        async def handler(m, baggage: CurrentBaggage) -> None:
            assert len(baggage.get_all_batch()) == expected_msg_count
            assert baggage.get_all() == expected_baggage
            m.sort()
            mock(m)
            event.set()

        async with self.patch_broker(broker) as br:
            tasks = (
                asyncio.create_task(
                    br.publish(
                        "hi",
                        list=queue,
                        headers=Baggage({"foo": "bar"}).to_headers(),
                    ),
                ),
                asyncio.create_task(
                    br.publish(
                        "buy",
                        list=queue,
                        headers=Baggage({"bar": "baz"}).to_headers(),
                    ),
                ),
            )
            await asyncio.wait(tasks, timeout=self.timeout)
            await broker.start()
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=self.timeout,
            )

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


@pytest.mark.redis()
class TestPublishWithTelemetry(TestPublish):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> RedisBroker:
        return RedisBroker(
            middlewares=(RedisTelemetryMiddleware(),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.redis()
class TestConsumeWithTelemetry(TestConsume):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> RedisBroker:
        return RedisBroker(
            middlewares=(RedisTelemetryMiddleware(),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.redis()
class TestConsumeListWithTelemetry(TestConsumeList):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> RedisBroker:
        return RedisBroker(
            middlewares=(RedisTelemetryMiddleware(),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.redis()
class TestConsumeStreamWithTelemetry(TestConsumeStream):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> RedisBroker:
        return RedisBroker(
            middlewares=(RedisTelemetryMiddleware(),),
            apply_types=apply_types,
            **kwargs,
        )
