import pytest

from faststream import TestApp
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)


@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_ml_lifespan() -> None:
    from docs.docs_src.getting_started.lifespan.rabbit.ml import app, broker, predict
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app):
        assert await (await broker.request(1.0, "test")).decode() == {"result": 42.0}

        predict.mock.assert_called_once_with(1.0)


@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_ml_lifespan() -> None:
    from docs.docs_src.getting_started.lifespan.kafka.ml import app, broker, predict
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker), TestApp(app):
        assert await (await broker.request(1.0, "test")).decode() == {"result": 42.0}

        predict.mock.assert_called_once_with(1.0)


@pytest.mark.asyncio()
@require_confluent
async def test_confluent_ml_lifespan() -> None:
    from docs.docs_src.getting_started.lifespan.confluent.ml import app, broker, predict
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker), TestApp(app):
        assert await (await broker.request(1.0, "test")).decode() == {"result": 42.0}

        predict.mock.assert_called_once_with(1.0)


@pytest.mark.asyncio()
@require_nats
async def test_nats_ml_lifespan() -> None:
    from docs.docs_src.getting_started.lifespan.nats.ml import app, broker, predict
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker), TestApp(app):
        assert await (await broker.request(1.0, "test")).decode() == {"result": 42.0}

        predict.mock.assert_called_once_with(1.0)


@pytest.mark.asyncio()
@require_redis
async def test_redis_ml_lifespan() -> None:
    from docs.docs_src.getting_started.lifespan.redis.ml import app, broker, predict
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker), TestApp(app):
        assert await (await broker.request(1.0, "test")).decode() == {"result": 42.0}

        predict.mock.assert_called_once_with(1.0)
