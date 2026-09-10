import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg
import pytest
import redis.asyncio as redis
from metrics import registry, tracer_provider
from opentelemetry import metrics, trace
from opentelemetry.semconv._incubating.attributes import messaging_attributes
from schemas.pydantic import Schema
from sql import DSN

from benchmarks.sql import find_user_by_name
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
from faststream.redis import RedisBroker
from faststream.redis.opentelemetry import RedisTelemetryMiddleware
from faststream.redis.prometheus import RedisPrometheusMiddleware


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamRedisSQLCase:
    comment = "Consume Messages with Metrics"
    broker_type = "Redis"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = RedisBroker(
            logger=None,
            graceful_timeout=10,
            middlewares=[
                RedisPrometheusMiddleware(registry=registry),
                RedisTelemetryMiddleware(tracer_provider=tracer_provider),
            ],
        )

        p = self.publisher = broker.publisher("in")
        self.sql_pool = await asyncpg.create_pool(dsn=DSN)

        @p
        @broker.subscriber("in")
        async def handle(message: Schema) -> Schema:
            self.EVENTS_PROCESSED += 1
            await find_user_by_name(message.name, self.sql_pool)
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
class TestPureRedisSQLCase:
    comment = "Pure redis client with metrics"
    broker_type = "Redis"

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
        self.sql_pool = await asyncpg.create_pool(dsn=DSN)

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        client = redis.Redis(host="localhost", port=6379, decode_responses=False)
        pubsub = client.pubsub()
        await pubsub.subscribe("in")

        metrics_manager = self.metrics
        tracer = self.tracer

        async def handler() -> None:
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                self.EVENTS_PROCESSED += 1
                body = msg["data"]
                metrics_manager.add_received_message(broker="redis", handler="in")
                metrics_manager.observe_received_messages_size(
                    broker="redis", handler="in", size=len(body)
                )
                metrics_manager.add_received_message_in_process(
                    broker="redis", handler="in"
                )

                trace_attributes = {
                    messaging_attributes.MESSAGING_SYSTEM: "redis",
                    messaging_attributes.MESSAGING_MESSAGE_BODY_SIZE: len(body),
                    MESSAGING_DESTINATION_PUBLISH_NAME: "in",
                }
                metrics_attributes = {
                    messaging_attributes.MESSAGING_SYSTEM: "redis",
                    MESSAGING_DESTINATION_PUBLISH_NAME: "in",
                }

                err: Exception | None = None
                start_time = time.perf_counter()
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
                        validated = Schema(**data)
                        await find_user_by_name(validated.name, self.sql_pool)
                        await client.publish("in", validated.model_dump_json())
                except Exception as e:
                    err = e
                    metrics_attributes[ERROR_TYPE] = type(e).__name__
                    metrics_manager.add_received_processed_message_exception(
                        broker="redis", handler="in", exception_type=type(e).__name__
                    )
                    raise
                finally:
                    duration = time.perf_counter() - start_time

                    self.process_duration.record(duration, attributes=metrics_attributes)
                    self.process_counter.add(
                        1,
                        attributes={
                            k: v for k, v in metrics_attributes.items() if k != ERROR_TYPE
                        },
                    )

                    metrics_manager.observe_received_processed_message_duration(
                        duration=duration,
                        broker="redis",
                        handler="in",
                    )
                    metrics_manager.remove_received_message_in_process(
                        broker="redis", handler="in"
                    )
                    metrics_manager.add_received_processed_message(
                        broker="redis",
                        handler="in",
                        status=ProcessingStatus.error if err else ProcessingStatus.acked,
                    )

        start_time = time.time()

        await client.publish(
            "in",
            json.dumps({
                "name": "John",
                "age": 39,
                "fullname": "LongString" * 8,
                "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
            }),
        )

        handler_task = asyncio.create_task(handler())

        try:
            yield start_time
        finally:
            handler_task.cancel()
            await pubsub.unsubscribe("in")
            await pubsub.aclose()
            await client.aclose()

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 1
