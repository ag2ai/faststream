import pytest

from faststream import TestApp
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_basic_lifespan() -> None:
    from docs.docs_src.getting_started.lifespan.kafka.basic import app, broker
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker), TestApp(app, {"env": ""}):
        assert app.context.get("settings").host == "localhost:9092"


@pytest.mark.confluent()
@pytest.mark.asyncio()
@require_confluent
async def test_confluent_basic_lifespan() -> None:
    from docs.docs_src.getting_started.lifespan.confluent.basic import app, broker
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker), TestApp(app, {"env": ""}):
        assert app.context.get("settings").host == "localhost:9092"


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_basic_lifespan() -> None:
    from docs.docs_src.getting_started.lifespan.rabbit.basic import app, broker
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app, {"env": ""}):
        assert app.context.get("settings").host == "amqp://guest:guest@localhost:5672/"


@pytest.mark.nats()
@pytest.mark.asyncio()
@require_nats
async def test_nats_basic_lifespan() -> None:
    from docs.docs_src.getting_started.lifespan.nats.basic import app, broker
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker), TestApp(app, {"env": ""}):
        assert app.context.get("settings").host == "nats://localhost:4222"


@pytest.mark.redis()
@pytest.mark.asyncio()
@require_redis
async def test_redis_basic_lifespan() -> None:
    from docs.docs_src.getting_started.lifespan.redis.basic import app, broker
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker), TestApp(app, {"env": ""}):
        assert app.context.get("settings").host == "redis://localhost:6379"
