import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from metrics import registry, tracer_provider
from opentelemetry import metrics, trace
from opentelemetry.semconv._incubating.attributes import messaging_attributes
from schemas.pydantic import Schema

from faststream._internal.utils.functions import run_in_executor
from faststream.confluent import KafkaBroker
from faststream.confluent.opentelemetry import KafkaTelemetryMiddleware
from faststream.confluent.prometheus import KafkaPrometheusMiddleware
from faststream.opentelemetry.consts import (
    ERROR_TYPE,
    INSTRUMENTING_LIBRARY_VERSION,
    INSTRUMENTING_MODULE_NAME,
    MESSAGING_DESTINATION_PUBLISH_NAME,
    OTEL_SCHEMA,
    MessageAction,
)
from faststream.prometheus.container import MetricsContainer
from faststream.prometheus.manager import MetricsManager
from faststream.prometheus.types import ProcessingStatus

MESSAGING_SYSTEM = "kafka"


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamConfluentMetricsCase:
    comment = "Consume Messages with Metrics"
    broker_type = "Confluent"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = KafkaBroker(
            logger=None,
            graceful_timeout=10,
            middlewares=[
                KafkaPrometheusMiddleware(registry=registry),
                KafkaTelemetryMiddleware(tracer_provider=tracer_provider),
            ],
        )

        p = self.publisher = broker.publisher("in")

        @p
        @broker.subscriber("in")
        async def handle(message: Schema) -> Schema:
            self.EVENTS_PROCESSED += 1
            return message

        self.handler = handle

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        async with self.broker:
            await self.broker.start()
            start_time = time.time()

            await self.publisher.publish({
                "name": "John",
                "age": 39,
                "fullname": "LongString" * 8,
                "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
            })

            yield start_time

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(6.0)
        assert self.EVENTS_PROCESSED > 1


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestPureConfluentMetricsCase:
    comment = "Pure confluent client with metrics"
    broker_type = "Confluent"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

        self.producer = Producer({
            "bootstrap.servers": "localhost:9092",
        })

        self.consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
            "enable.auto.commit": True,
            "auto.offset.reset": "earliest",
        })
        self.consumer.assign([TopicPartition("in", 0, 0)])

        container = MetricsContainer(registry, custom_label_names=())
        self.metrics = MetricsManager(container, app_name="faststream")

        self.tracer = trace.get_tracer(
            INSTRUMENTING_MODULE_NAME,
            INSTRUMENTING_LIBRARY_VERSION,
            tracer_provider=tracer_provider,
            schema_url=OTEL_SCHEMA,
        )
        meter = metrics.get_meter(__name__, schema_url=OTEL_SCHEMA)
        self.process_duration = meter.create_histogram(
            name="messaging.process.duration",
            unit="s",
            description="Measures the duration of process operation.",
        )
        self.process_counter = meter.create_counter(
            name="messaging.process.messages",
            unit="message",
            description="Measures the number of processed messages.",
        )

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:  # noqa: PLR0915
        stop_event = asyncio.Event()

        metrics_manager = self.metrics
        tracer = self.tracer

        def acked(err, msg) -> None:  # noqa: ANN001
            if err is not None:
                print(f"Failed to deliver message: {msg!s}: {err!s}")

        def handle() -> None:
            while not stop_event.is_set():
                try:
                    msg = self.consumer.poll(timeout=0.01)
                except RuntimeError:
                    break
                if msg is None:
                    continue
                self.EVENTS_PROCESSED += 1

                body = msg.value()
                metrics_manager.add_received_message(
                    broker=MESSAGING_SYSTEM, handler="in"
                )
                metrics_manager.observe_received_messages_size(
                    broker=MESSAGING_SYSTEM, handler="in", size=len(body)
                )
                metrics_manager.add_received_message_in_process(
                    broker=MESSAGING_SYSTEM, handler="in"
                )

                trace_attributes = {
                    messaging_attributes.MESSAGING_SYSTEM: MESSAGING_SYSTEM,
                    messaging_attributes.MESSAGING_MESSAGE_BODY_SIZE: len(body),
                    MESSAGING_DESTINATION_PUBLISH_NAME: "in",
                }
                metrics_attributes = {
                    messaging_attributes.MESSAGING_SYSTEM: MESSAGING_SYSTEM,
                    MESSAGING_DESTINATION_PUBLISH_NAME: "in",
                }

                err: Exception | None = None
                started_at = time.perf_counter()
                try:
                    with tracer.start_as_current_span(
                        name=f"in {MessageAction.PROCESS}",
                        kind=trace.SpanKind.CONSUMER,
                        attributes=trace_attributes,
                    ) as span:
                        span.set_attribute(
                            messaging_attributes.MESSAGING_OPERATION_TYPE,
                            MessageAction.PROCESS,
                        )
                        data = json.loads(body.decode("utf-8"))
                        parsed = Schema(**data)
                        self.producer.produce(
                            "in",
                            value=parsed.model_dump_json().encode("utf-8"),
                            callback=acked,
                        )
                        self.producer.flush()
                except Exception as e:
                    err = e
                    metrics_attributes[ERROR_TYPE] = type(e).__name__
                    metrics_manager.add_received_processed_message_exception(
                        broker=MESSAGING_SYSTEM,
                        handler="in",
                        exception_type=type(e).__name__,
                    )
                    raise
                finally:
                    duration = time.perf_counter() - started_at

                    self.process_duration.record(duration, attributes=metrics_attributes)
                    self.process_counter.add(
                        1,
                        attributes={
                            k: v for k, v in metrics_attributes.items() if k != ERROR_TYPE
                        },
                    )

                    metrics_manager.observe_received_processed_message_duration(
                        duration=duration,
                        broker=MESSAGING_SYSTEM,
                        handler="in",
                    )
                    metrics_manager.remove_received_message_in_process(
                        broker=MESSAGING_SYSTEM, handler="in"
                    )
                    metrics_manager.add_received_processed_message(
                        broker=MESSAGING_SYSTEM,
                        handler="in",
                        status=ProcessingStatus.error if err else ProcessingStatus.acked,
                    )

        loop = asyncio.get_event_loop()
        start_time = time.time()
        executor_task = loop.run_in_executor(None, handle)

        value = json.dumps({
            "name": "John",
            "age": 39,
            "fullname": "LongString" * 8,
            "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
        }).encode("utf-8")

        await run_in_executor(None, self.producer.produce, "in", value=value)
        await run_in_executor(None, self.producer.poll, 0)

        try:
            yield start_time
        finally:
            stop_event.set()
            await executor_task
            await run_in_executor(None, self.producer.flush)
            await run_in_executor(None, self.consumer.close)

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(6.0)
        assert self.EVENTS_PROCESSED > 1
