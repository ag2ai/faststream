import pytest

from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)


@pytest.mark.asyncio()
@require_aiokafka
async def test_depends_kafka() -> None:
    from docs.docs_src.getting_started.dependencies.basic.kafka.depends import (
        broker,
        handler,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker):
        await broker.publish({}, "test")
        handler.mock.assert_called_once_with({})


@pytest.mark.asyncio()
@require_confluent
async def test_depends_confluent() -> None:
    from docs.docs_src.getting_started.dependencies.basic.confluent.depends import (
        broker,
        handler,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker):
        await broker.publish({}, "test")
        handler.mock.assert_called_once_with({})


@pytest.mark.asyncio()
@require_aiopika
async def test_depends_rabbit() -> None:
    from docs.docs_src.getting_started.dependencies.basic.rabbit.depends import (
        broker,
        handler,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker):
        await broker.publish({}, "test")
        handler.mock.assert_called_once_with({})


@pytest.mark.asyncio()
@require_nats
async def test_depends_nats() -> None:
    from docs.docs_src.getting_started.dependencies.basic.nats.depends import (
        broker,
        handler,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker):
        await broker.publish({}, "test")
        handler.mock.assert_called_once_with({})


@pytest.mark.asyncio()
@require_redis
async def test_depends_redis() -> None:
    from docs.docs_src.getting_started.dependencies.basic.redis.depends import (
        broker,
        handler,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker):
        await broker.publish({}, "test")
        handler.mock.assert_called_once_with({})
