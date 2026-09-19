import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import aio_pika
import pytest
from metrics import registry, tracer_provider
from opentelemetry import metrics, trace
from opentelemetry.semconv._incubating.attributes import messaging_attributes
from schemas.pydantic import Schema

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
from faststream.rabbit import RabbitBroker
from faststream.rabbit.opentelemetry import RabbitTelemetryMiddleware
from faststream.rabbit.prometheus import RabbitPrometheusMiddleware

from .test_basic import RABBIT_URL, prefill_queue

MESSAGING_SYSTEM = "rabbitmq"


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamRabbitMetricsCase:
    comment = "Consume Messages with Metrics"
    broker_type = "RabbitMQ"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0
        self.PREFILL_MESSAGES = prefill_messages

        broker = self.broker = RabbitBroker(
            logger=None,
            graceful_timeout=10,
            middlewares=[
                RabbitPrometheusMiddleware(registry=registry),
                RabbitTelemetryMiddleware(tracer_provider=tracer_provider),
            ],
        )

        @broker.subscriber("in")
        async def handle(message: Schema) -> Schema:
            self.EVENTS_PROCESSED += 1
            return message

        self.handler = handle

        await prefill_queue(RABBIT_URL, self.PREFILL_MESSAGES)

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        async with self.broker:
            await self.broker.start()
            start_time = time.time()

            yield start_time

    async def test_consume_message(self) -> None:
        async with self.start():
            while self.EVENTS_PROCESSED < self.PREFILL_MESSAGES:
                await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED == self.PREFILL_MESSAGES


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestPureRabbitMetricsCase:
    comment = "Pure aio-pika with metrics"
    broker_type = "RabbitMQ"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0
        self.PREFILL_MESSAGES = prefill_messages

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

        await prefill_queue(RABBIT_URL, self.PREFILL_MESSAGES)

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        connection = await aio_pika.connect_robust(RABBIT_URL)
        channel = await connection.channel()

        metrics_manager = self.metrics
        tracer = self.tracer

        async def handler(msg: aio_pika.IncomingMessage) -> None:
            async with msg.process():
                self.EVENTS_PROCESSED += 1

                body = msg.body
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
                        Schema(**data)
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

        queue = await channel.declare_queue("in", durable=True)
        await queue.consume(handler)

        start_time = time.time()

        yield start_time
        await connection.close()

    async def test_consume_message(self) -> None:
        async with self.start():
            while self.EVENTS_PROCESSED < self.PREFILL_MESSAGES:
                await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED == self.PREFILL_MESSAGES
