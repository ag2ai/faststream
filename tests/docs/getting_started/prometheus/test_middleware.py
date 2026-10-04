import pytest
from prometheus_client import CollectorRegistry
from starlette.testclient import TestClient

from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)

PUBLISHED_TOTAL = "faststream_published_messages_total"
RECEIVED_TOTAL = "faststream_received_messages_total"


async def handle(msg: str) -> None:
    # the snippets wire the middleware to a broker without handlers,
    # so the subscriber a metric needs comes from the test
    ...


def message_totals(registry: CollectorRegistry) -> dict[str, float]:
    return {
        sample.name: sample.value
        for metric in registry.collect()
        for sample in metric.samples
        if sample.name in {PUBLISHED_TOTAL, RECEIVED_TOTAL}
    }


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_middleware() -> None:
    from docs.docs_src.getting_started.prometheus.kafka import broker, registry
    from faststream.kafka import TestKafkaBroker

    broker.subscriber("test-topic")(handle)

    async with TestKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

    assert message_totals(registry) == {PUBLISHED_TOTAL: 1.0, RECEIVED_TOTAL: 1.0}


@pytest.mark.confluent()
@pytest.mark.asyncio()
@require_confluent
async def test_confluent_middleware() -> None:
    from docs.docs_src.getting_started.prometheus.confluent import broker, registry
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    broker.subscriber("test-topic")(handle)

    async with TestConfluentKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

    assert message_totals(registry) == {PUBLISHED_TOTAL: 1.0, RECEIVED_TOTAL: 1.0}


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_middleware() -> None:
    from docs.docs_src.getting_started.prometheus.rabbit import broker, registry
    from faststream.rabbit import TestRabbitBroker

    broker.subscriber("test-queue")(handle)

    async with TestRabbitBroker(broker) as br:
        await br.publish("Hi!", "test-queue")

    assert message_totals(registry) == {PUBLISHED_TOTAL: 1.0, RECEIVED_TOTAL: 1.0}


@pytest.mark.nats()
@pytest.mark.asyncio()
@require_nats
async def test_nats_middleware() -> None:
    from docs.docs_src.getting_started.prometheus.nats import broker, registry
    from faststream.nats import TestNatsBroker

    broker.subscriber("test-subject")(handle)

    async with TestNatsBroker(broker) as br:
        await br.publish("Hi!", "test-subject")

    assert message_totals(registry) == {PUBLISHED_TOTAL: 1.0, RECEIVED_TOTAL: 1.0}


@pytest.mark.redis()
@pytest.mark.asyncio()
@require_redis
async def test_redis_middleware() -> None:
    from docs.docs_src.getting_started.prometheus.redis import broker, registry
    from faststream.redis import TestRedisBroker

    broker.subscriber("test-channel")(handle)

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", "test-channel")

    assert message_totals(registry) == {PUBLISHED_TOTAL: 1.0, RECEIVED_TOTAL: 1.0}


@pytest.mark.mqtt()
@pytest.mark.asyncio()
@require_mqtt
async def test_mqtt_middleware() -> None:
    from docs.docs_src.getting_started.prometheus.mqtt import broker, registry
    from faststream.mqtt import TestMQTTBroker

    broker.subscriber("test-topic")(handle)

    async with TestMQTTBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

    assert message_totals(registry) == {PUBLISHED_TOTAL: 1.0, RECEIVED_TOTAL: 1.0}


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_asgi_metrics() -> None:
    from docs.docs_src.getting_started.prometheus.kafka_asgi import app, broker
    from faststream.kafka import TestKafkaBroker

    broker.subscriber("test-topic")(handle)

    async with TestKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

        with TestClient(app) as client:
            response = client.get("/metrics")

    assert response.status_code == 200
    assert f"{PUBLISHED_TOTAL}{{" in response.text


@pytest.mark.confluent()
@pytest.mark.asyncio()
@require_confluent
async def test_confluent_asgi_metrics() -> None:
    from docs.docs_src.getting_started.prometheus.confluent_asgi import app, broker
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    broker.subscriber("test-topic")(handle)

    async with TestConfluentKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

        with TestClient(app) as client:
            response = client.get("/metrics")

    assert response.status_code == 200
    assert f"{PUBLISHED_TOTAL}{{" in response.text


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_asgi_metrics() -> None:
    from docs.docs_src.getting_started.prometheus.rabbit_asgi import app, broker
    from faststream.rabbit import TestRabbitBroker

    broker.subscriber("test-queue")(handle)

    async with TestRabbitBroker(broker) as br:
        await br.publish("Hi!", "test-queue")

        with TestClient(app) as client:
            response = client.get("/metrics")

    assert response.status_code == 200
    assert f"{PUBLISHED_TOTAL}{{" in response.text


@pytest.mark.nats()
@pytest.mark.asyncio()
@require_nats
async def test_nats_asgi_metrics() -> None:
    from docs.docs_src.getting_started.prometheus.nats_asgi import app, broker
    from faststream.nats import TestNatsBroker

    broker.subscriber("test-subject")(handle)

    async with TestNatsBroker(broker) as br:
        await br.publish("Hi!", "test-subject")

        with TestClient(app) as client:
            response = client.get("/metrics")

    assert response.status_code == 200
    assert f"{PUBLISHED_TOTAL}{{" in response.text


@pytest.mark.redis()
@pytest.mark.asyncio()
@require_redis
async def test_redis_asgi_metrics() -> None:
    from docs.docs_src.getting_started.prometheus.redis_asgi import app, broker
    from faststream.redis import TestRedisBroker

    broker.subscriber("test-channel")(handle)

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", "test-channel")

        with TestClient(app) as client:
            response = client.get("/metrics")

    assert response.status_code == 200
    assert f"{PUBLISHED_TOTAL}{{" in response.text


@pytest.mark.mqtt()
@pytest.mark.asyncio()
@require_mqtt
async def test_mqtt_asgi_metrics() -> None:
    from docs.docs_src.getting_started.prometheus.mqtt_asgi import app, broker
    from faststream.mqtt import TestMQTTBroker

    broker.subscriber("test-topic")(handle)

    async with TestMQTTBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

        with TestClient(app) as client:
            response = client.get("/metrics")

    assert response.status_code == 200
    assert f"{PUBLISHED_TOTAL}{{" in response.text
