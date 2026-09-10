import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from metrics import registry, tracer_provider
from opentelemetry import metrics, trace
from opentelemetry.semconv._incubating.attributes import messaging_attributes
from schemas.pydantic import Schema

from faststream.kafka import KafkaBroker
from faststream.kafka.opentelemetry import KafkaTelemetryMiddleware
from faststream.kafka.prometheus import KafkaPrometheusMiddleware
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
class TestFaststreamKafkaMetricsCase:
    comment = "Consume Messages with Metrics"
    broker_type = "Kafka"

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
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 1


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestPureKafkaMetricsCase:
    comment = "Pure aio-kafka client with metrics"
    broker_type = "Kafka"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

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

    async def create_topic(self) -> None:
        admin = AIOKafkaAdminClient(bootstrap_servers="localhost:9092")
        await admin.start()
        try:
            with suppress(Exception):
                await admin.create_topics([
                    NewTopic(name="in", num_partitions=1, replication_factor=1)
                ])
        finally:
            await admin.close()

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:  # noqa: PLR0915
        await self.create_topic()
        producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
        consumer = AIOKafkaConsumer(
            "in",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await producer.start()
        await consumer.start()

        metrics_manager = self.metrics
        tracer = self.tracer

        start_time = time.time()
        stop_event = asyncio.Event()

        await producer.send_and_wait(
            "in",
            json.dumps({
                "name": "John",
                "age": 39,
                "fullname": "LongString" * 8,
                "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
            }).encode(),
        )

        async def message_loop() -> None:
            try:
                async for msg in consumer:
                    if stop_event.is_set():
                        break
                    self.EVENTS_PROCESSED += 1

                    body = msg.value
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
                            data = json.loads(body.decode())
                            parsed = Schema(**data)
                            await producer.send_and_wait(
                                "in", parsed.model_dump_json().encode()
                            )
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

                        self.process_duration.record(
                            duration, attributes=metrics_attributes
                        )
                        self.process_counter.add(
                            1,
                            attributes={
                                k: v
                                for k, v in metrics_attributes.items()
                                if k != ERROR_TYPE
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
                            status=ProcessingStatus.error
                            if err
                            else ProcessingStatus.acked,
                        )
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(message_loop())
        try:
            yield start_time
        finally:
            stop_event.set()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            await producer.stop()
            await consumer.stop()

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 1
