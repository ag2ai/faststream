import importlib.util
import runpy
from typing import Any

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)


async def handle(msg: str) -> None:
    # the snippets wire the middleware to a broker without handlers,
    # so the subscriber a `process` span needs comes from the test
    ...


class Telemetry:
    def __init__(self) -> None:
        self.exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer_provider = provider

    def run_snippet(self, module: str) -> Any:
        # the snippets are page fragments: `tracer_provider` is built by
        # the code block the page shows above them
        spec = importlib.util.find_spec(module)
        assert spec is not None
        assert spec.origin is not None

        namespace = runpy.run_path(
            spec.origin,
            init_globals={"tracer_provider": self.tracer_provider},
        )
        return namespace["broker"]

    def span_names(self) -> list[str]:
        spans = sorted(
            self.exporter.get_finished_spans(),
            key=lambda span: span.start_time or 0,
        )
        return [span.name or "" for span in spans]


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_telemetry() -> None:
    from faststream.kafka import TestKafkaBroker

    telemetry = Telemetry()
    broker = telemetry.run_snippet(
        "docs.docs_src.getting_started.opentelemetry.kafka_telemetry",
    )
    broker.subscriber("test-topic")(handle)

    async with TestKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

    assert telemetry.span_names() == [
        "test-topic create",
        "test-topic publish",
        "test-topic process",
    ]


@pytest.mark.confluent()
@pytest.mark.asyncio()
@require_confluent
async def test_confluent_telemetry() -> None:
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    telemetry = Telemetry()
    broker = telemetry.run_snippet(
        "docs.docs_src.getting_started.opentelemetry.confluent_telemetry",
    )
    broker.subscriber("test-topic")(handle)

    async with TestConfluentKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

    assert telemetry.span_names() == [
        "test-topic create",
        "test-topic publish",
        "test-topic process",
    ]


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_telemetry() -> None:
    from faststream.rabbit import TestRabbitBroker

    telemetry = Telemetry()
    broker = telemetry.run_snippet(
        "docs.docs_src.getting_started.opentelemetry.rabbit_telemetry",
    )
    broker.subscriber("test-queue")(handle)

    async with TestRabbitBroker(broker) as br:
        await br.publish("Hi!", "test-queue")

    # RabbitMQ spans are named after the default exchange the queue is bound to
    assert telemetry.span_names() == [
        "default.test-queue create",
        "default.test-queue publish",
        "default.test-queue process",
    ]


@pytest.mark.nats()
@pytest.mark.asyncio()
@require_nats
async def test_nats_telemetry() -> None:
    from faststream.nats import TestNatsBroker

    telemetry = Telemetry()
    broker = telemetry.run_snippet(
        "docs.docs_src.getting_started.opentelemetry.nats_telemetry",
    )
    broker.subscriber("test-subject")(handle)

    async with TestNatsBroker(broker) as br:
        await br.publish("Hi!", "test-subject")

    assert telemetry.span_names() == [
        "test-subject create",
        "test-subject publish",
        "test-subject process",
    ]


@pytest.mark.redis()
@pytest.mark.asyncio()
@require_redis
async def test_redis_telemetry() -> None:
    from faststream.redis import TestRedisBroker

    telemetry = Telemetry()
    broker = telemetry.run_snippet(
        "docs.docs_src.getting_started.opentelemetry.redis_telemetry",
    )
    broker.subscriber("test-channel")(handle)

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", "test-channel")

    assert telemetry.span_names() == [
        "test-channel create",
        "test-channel publish",
        "test-channel process",
    ]


@pytest.mark.mqtt()
@pytest.mark.asyncio()
@require_mqtt
async def test_mqtt_telemetry() -> None:
    from faststream.mqtt import TestMQTTBroker

    telemetry = Telemetry()
    broker = telemetry.run_snippet(
        "docs.docs_src.getting_started.opentelemetry.mqtt_telemetry",
    )
    broker.subscriber("test-topic")(handle)

    async with TestMQTTBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

    assert telemetry.span_names() == [
        "test-topic create",
        "test-topic publish",
        "test-topic process",
    ]
