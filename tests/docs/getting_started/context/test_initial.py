import pytest

from tests.marks import (
    python39,
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)


@pytest.mark.asyncio()
@python39
@require_aiokafka
async def test_kafka() -> None:
    from docs.docs_src.getting_started.context.kafka.initial import broker
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker) as br:
        await br.publish("", "test-topic")
        await br.publish("", "test-topic")

    assert broker.context.get("collector") == ["", ""]
    broker.context.clear()


@pytest.mark.asyncio()
@python39
@require_confluent
async def test_confluent() -> None:
    from docs.docs_src.getting_started.context.confluent.initial import broker
    from faststream.confluent import TestKafkaBroker

    async with TestKafkaBroker(broker) as br:
        await br.publish("", "test-topic")
        await br.publish("", "test-topic")

    assert broker.context.get("collector") == ["", ""]
    broker.context.clear()


@pytest.mark.asyncio()
@python39
@require_aiopika
async def test_rabbit() -> None:
    from docs.docs_src.getting_started.context.rabbit.initial import broker
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker) as br:
        await br.publish("", "test-queue")
        await br.publish("", "test-queue")

    assert broker.context.get("collector") == ["", ""]
    broker.context.clear()


@pytest.mark.asyncio()
@python39
@require_nats
async def test_nats() -> None:
    from docs.docs_src.getting_started.context.nats.initial import broker
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker) as br:
        await br.publish("", "test-subject")
        await br.publish("", "test-subject")

    assert broker.context.get("collector") == ["", ""]
    broker.context.clear()


@pytest.mark.asyncio()
@python39
@require_redis
async def test_redis() -> None:
    from docs.docs_src.getting_started.context.redis.initial import broker
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker) as br:
        await br.publish("", "test-channel")
        await br.publish("", "test-channel")

    assert broker.context.get("collector") == ["", ""]
    broker.context.clear()
