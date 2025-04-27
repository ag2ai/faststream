import asyncio
from typing import Any, Optional, cast
from unittest.mock import Mock

import pytest
from dirty_equals import IsFloat, IsUUID
from opentelemetry import baggage, context
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.point import Metric
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Span, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv.trace import SpanAttributes as SpanAttr
from opentelemetry.trace import SpanKind, get_current_span

from faststream._internal.broker.broker import BrokerUsecase
from faststream.opentelemetry import Baggage, CurrentBaggage, CurrentSpan
from faststream.opentelemetry.consts import (
    ERROR_TYPE,
    MESSAGING_DESTINATION_PUBLISH_NAME,
)
from faststream.opentelemetry.middleware import (
    MessageAction as Action,
    TelemetryMiddleware,
)
from tests.brokers.base.basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class LocalTelemetryTestcase(BaseTestcaseConfig):
    messaging_system: str
    include_messages_counters: bool
    resource: Resource = Resource.create(attributes={"service.name": "faststream.test"})
    telemetry_middleware_class: TelemetryMiddleware

    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> BrokerUsecase[Any, Any]:
        raise NotImplementedError

    def patch_broker(
        self,
        broker: BrokerUsecase[Any, Any],
        **kwargs: Any,
    ) -> BrokerUsecase[Any, Any]:
        return broker

    def destination_name(self, queue: str) -> str:
        return queue

    @staticmethod
    def get_spans(exporter: InMemorySpanExporter) -> list[Span]:
        spans = cast("tuple[Span, ...]", exporter.get_finished_spans())
        return sorted(spans, key=lambda s: s.start_time or 0)

    @staticmethod
    def get_metrics(
        reader: InMemoryMetricReader,
    ) -> list[Metric]:
        """Get sorted metrics.

        Return order:
            - messaging.process.duration
            - messaging.process.messages
            - messaging.publish.duration
            - messaging.publish.messages
        """
        metrics = reader.get_metrics_data()
        metrics = metrics.resource_metrics[0].scope_metrics[0].metrics
        metrics = sorted(metrics, key=lambda m: m.name)
        return cast("list[Metric]", metrics)

    @pytest.fixture()
    def tracer_provider(self) -> TracerProvider:
        return TracerProvider(resource=self.resource)

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
        return MeterProvider(metric_readers=(metric_reader,), resource=self.resource)

    def assert_span(
        self,
        span: Span,
        action: str,
        queue: str,
        msg: str,
        parent_span_id: Optional[str] = None,
    ) -> None:
        attrs = span.attributes or {}
        assert attrs[SpanAttr.MESSAGING_SYSTEM] == self.messaging_system, attrs[
            SpanAttr.MESSAGING_SYSTEM
        ]
        assert attrs[SpanAttr.MESSAGING_MESSAGE_CONVERSATION_ID] == IsUUID, attrs[
            SpanAttr.MESSAGING_MESSAGE_CONVERSATION_ID
        ]
        assert span.name == f"{self.destination_name(queue)} {action}", span.name
        assert span.kind in {SpanKind.CONSUMER, SpanKind.PRODUCER}, span.kind

        if span.kind == SpanKind.PRODUCER and action in {Action.CREATE, Action.PUBLISH}:
            assert attrs[SpanAttr.MESSAGING_DESTINATION_NAME] == queue, attrs[
                SpanAttr.MESSAGING_DESTINATION_NAME
            ]

        if span.kind == SpanKind.CONSUMER and action in {Action.CREATE, Action.PROCESS}:
            assert attrs[MESSAGING_DESTINATION_PUBLISH_NAME] == queue, attrs[
                MESSAGING_DESTINATION_PUBLISH_NAME
            ]
            assert attrs[SpanAttr.MESSAGING_MESSAGE_ID] == IsUUID, attrs[
                SpanAttr.MESSAGING_MESSAGE_ID
            ]

        if action == Action.PROCESS:
            assert attrs[SpanAttr.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES] == len(msg), (
                attrs[SpanAttr.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES]
            )
            assert attrs[SpanAttr.MESSAGING_OPERATION] == action, attrs[
                SpanAttr.MESSAGING_OPERATION
            ]

        if action == Action.PUBLISH:
            assert attrs[SpanAttr.MESSAGING_OPERATION] == action, attrs[
                SpanAttr.MESSAGING_OPERATION
            ]

        if parent_span_id:
            assert span.parent
            assert span.parent.span_id == parent_span_id, span.parent.span_id

    def assert_metrics(
        self,
        metrics: list[Metric],
        count: int = 1,
        error_type: Optional[str] = None,
    ) -> None:
        if self.include_messages_counters:
            assert len(metrics) == 4
            proc_dur, proc_msg, pub_dur, pub_msg = metrics

            assert proc_msg.data.data_points[0].value == count
            assert pub_msg.data.data_points[0].value == count

        else:
            assert len(metrics) == 2
            proc_dur, pub_dur = metrics

        if error_type:
            assert proc_dur.data.data_points[0].attributes[ERROR_TYPE] == error_type

        assert proc_dur.data.data_points[0].count == 1
        assert proc_dur.data.data_points[0].sum == IsFloat

        assert pub_dur.data.data_points[0].count == 1
        assert pub_dur.data.data_points[0].sum == IsFloat

    async def test_subscriber_create_publish_process_span(
        self,
        queue: str,
        mock: Mock,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
    ) -> None:
        event = asyncio.Event()

        mid = self.telemetry_middleware_class(tracer_provider=tracer_provider)
        broker = self.get_broker(middlewares=(mid,))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(m) -> None:
            mock(m)
            event.set()

        broker = self.patch_broker(broker)
        msg = "start"

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(broker.publish(msg, queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        create, publish, process = self.get_spans(trace_exporter)
        parent_span_id = create.context.span_id

        self.assert_span(create, Action.CREATE, queue, msg)
        self.assert_span(publish, Action.PUBLISH, queue, msg, parent_span_id)
        self.assert_span(process, Action.PROCESS, queue, msg, parent_span_id)

        assert event.is_set()
        mock.assert_called_once_with(msg)

    async def test_chain_subscriber_publisher(
        self,
        queue: str,
        mock: Mock,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
    ) -> None:
        event = asyncio.Event()

        mid = self.telemetry_middleware_class(tracer_provider=tracer_provider)
        broker = self.get_broker(middlewares=(mid,))

        first_queue = queue
        second_queue = queue + "2"

        args, kwargs = self.get_subscriber_params(first_queue)

        @broker.subscriber(*args, **kwargs)
        @broker.publisher(second_queue)
        async def handler1(m):
            return m

        args2, kwargs2 = self.get_subscriber_params(second_queue)

        @broker.subscriber(*args2, **kwargs2)
        async def handler2(m) -> None:
            mock(m)
            event.set()

        broker = self.patch_broker(broker)
        msg = "start"

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(broker.publish(msg, queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        spans = self.get_spans(trace_exporter)
        create, pub1, proc1, pub2, proc2 = spans
        parent_span_id = create.context.span_id

        self.assert_span(create, Action.CREATE, first_queue, msg)
        self.assert_span(pub1, Action.PUBLISH, first_queue, msg, parent_span_id)
        self.assert_span(proc1, Action.PROCESS, first_queue, msg, parent_span_id)
        self.assert_span(pub2, Action.PUBLISH, second_queue, msg, proc1.context.span_id)
        self.assert_span(proc2, Action.PROCESS, second_queue, msg, parent_span_id)

        assert (
            create.start_time
            < pub1.start_time
            < proc1.start_time
            < pub2.start_time
            < proc2.start_time
        )

        assert event.is_set()
        mock.assert_called_once_with(msg)

    async def test_no_trace_context_create_process_span(
        self,
        queue: str,
        mock: Mock,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
    ) -> None:
        event = asyncio.Event()

        mid = self.telemetry_middleware_class(tracer_provider=tracer_provider)
        broker = self.get_broker(middlewares=(mid,))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(m) -> None:
            mock(m)
            event.set()

        broker = self.patch_broker(broker)
        msg = "start"

        async with broker:
            await broker.start()
            broker.middlewares = ()
            tasks = (
                asyncio.create_task(broker.publish(msg, queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        create, process = self.get_spans(trace_exporter)
        parent_span_id = create.context.span_id

        self.assert_span(create, Action.CREATE, queue, msg)
        self.assert_span(process, Action.PROCESS, queue, msg, parent_span_id)

        assert event.is_set()
        mock.assert_called_once_with(msg)

    async def test_metrics(
        self,
        queue: str,
        mock: Mock,
        meter_provider: MeterProvider,
        metric_reader: InMemoryMetricReader,
    ) -> None:
        event = asyncio.Event()

        mid = self.telemetry_middleware_class(meter_provider=meter_provider)
        broker = self.get_broker(middlewares=(mid,))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(m) -> None:
            mock(m)
            event.set()

        broker = self.patch_broker(broker)
        msg = "start"

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(broker.publish(msg, queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        metrics = self.get_metrics(metric_reader)

        self.assert_metrics(metrics)

        assert event.is_set()
        mock.assert_called_once_with(msg)

    async def test_error_metrics(
        self,
        queue: str,
        mock: Mock,
        meter_provider: MeterProvider,
        metric_reader: InMemoryMetricReader,
    ) -> None:
        event = asyncio.Event()

        mid = self.telemetry_middleware_class(meter_provider=meter_provider)
        broker = self.get_broker(middlewares=(mid,))
        expected_value_type = "ValueError"

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(m) -> None:
            try:
                raise ValueError
            finally:
                mock(m)
                event.set()

        broker = self.patch_broker(broker)
        msg = "start"

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(broker.publish(msg, queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        metrics = self.get_metrics(metric_reader)

        self.assert_metrics(metrics, error_type=expected_value_type)

        assert event.is_set()
        mock.assert_called_once_with(msg)

    async def test_span_in_context(
        self,
        queue: str,
        mock: Mock,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
    ) -> None:
        event = asyncio.Event()

        mid = self.telemetry_middleware_class(tracer_provider=tracer_provider)
        broker = self.get_broker(middlewares=(mid,), apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(m, span: CurrentSpan) -> None:
            assert span is get_current_span()
            mock(m)
            event.set()

        broker = self.patch_broker(broker)
        msg = "start"

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(broker.publish(msg, queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        assert event.is_set()
        mock.assert_called_once_with(msg)

    async def test_get_baggage(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        mid = self.telemetry_middleware_class()
        broker = self.get_broker(middlewares=(mid,), apply_types=True)
        expected_baggage = {"foo": "bar"}

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler1(m, baggage: CurrentBaggage) -> None:
            assert baggage.get("foo") == "bar"
            assert baggage.get_all() == expected_baggage
            assert baggage.get_all_batch() == []
            assert baggage.__repr__() == expected_baggage.__repr__()
            mock(m)
            event.set()

        broker = self.patch_broker(broker)
        msg = "start"

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(
                    broker.publish(
                        msg,
                        queue,
                        headers=Baggage({"foo": "bar"}).to_headers(),
                    ),
                ),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        assert event.is_set()
        mock.assert_called_once_with(msg)

    async def test_clear_baggage(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        mid = self.telemetry_middleware_class()
        broker = self.get_broker(middlewares=(mid,), apply_types=True)

        first_queue = queue + "1"
        second_queue = queue + "2"

        args, kwargs = self.get_subscriber_params(first_queue)

        @broker.subscriber(*args, **kwargs)
        @broker.publisher(second_queue)
        async def handler1(m, baggage: CurrentBaggage):
            baggage.clear()
            assert baggage.get_all() == {}
            return m

        args2, kwargs2 = self.get_subscriber_params(second_queue)

        @broker.subscriber(*args2, **kwargs2)
        async def handler2(m, baggage: CurrentBaggage) -> None:
            assert baggage.get_all() == {}
            mock(m)
            event.set()

        broker = self.patch_broker(broker)
        msg = "start"

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(
                    broker.publish(
                        msg,
                        first_queue,
                        headers=Baggage({"foo": "bar"}).to_headers(),
                    ),
                ),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        assert event.is_set()
        mock.assert_called_once_with(msg)

    async def test_modify_baggage(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        mid = self.telemetry_middleware_class()
        broker = self.get_broker(middlewares=(mid,), apply_types=True)
        expected_baggage = {"baz": "bar", "bar": "baz"}

        first_queue = queue + "1"
        second_queue = queue + "2"

        args, kwargs = self.get_subscriber_params(first_queue)

        @broker.subscriber(*args, **kwargs)
        @broker.publisher(second_queue)
        async def handler1(m, baggage: CurrentBaggage):
            baggage.set("bar", "baz")
            baggage.set("baz", "bar")
            baggage.remove("foo")
            return m

        args2, kwargs2 = self.get_subscriber_params(second_queue)

        @broker.subscriber(*args2, **kwargs2)
        async def handler2(m, baggage: CurrentBaggage) -> None:
            assert baggage.get_all() == expected_baggage
            mock(m)
            event.set()

        broker = self.patch_broker(broker)
        msg = "start"

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(
                    broker.publish(
                        msg,
                        first_queue,
                        headers=Baggage({"foo": "bar"}).to_headers(),
                    ),
                ),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        assert event.is_set()
        mock.assert_called_once_with(msg)

    async def test_get_baggage_from_headers(
        self,
        queue: str,
    ):
        event = asyncio.Event()

        mid = self.telemetry_middleware_class()
        broker = self.get_broker(middlewares=(mid,), apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        expected_baggage = {"foo": "bar", "bar": "baz"}

        ctx = context.Context()
        for key, value in expected_baggage.items():
            ctx = baggage.set_baggage(key, value, context=ctx)

        propagator = W3CBaggagePropagator()
        headers = {}
        propagator.inject(headers, context=ctx)

        @broker.subscriber(*args, **kwargs)
        async def handler():
            baggage_instance = Baggage.from_headers(headers)
            extracted_baggage = baggage_instance.get_all()
            assert extracted_baggage == expected_baggage
            event.set()

        broker = self.patch_broker(broker)
        msg = "start"

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(broker.publish(msg, queue, headers=headers)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        assert event.is_set()
