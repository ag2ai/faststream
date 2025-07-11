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
@require_aiokafka
async def test_kafka_filtering() -> None:
    from docs.docs_src.getting_started.subscription.kafka.filter import (
        app,
        broker,
        default_handler,
        handle,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})
        default_handler.mock.assert_called_once_with("Hello, FastStream!")


@pytest.mark.asyncio()
@require_confluent
async def test_confluent_filtering() -> None:
    from docs.docs_src.getting_started.subscription.confluent.filter import (
        app,
        broker,
        default_handler,
        handle,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})
        default_handler.mock.assert_called_once_with("Hello, FastStream!")


@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_filtering() -> None:
    from docs.docs_src.getting_started.subscription.rabbit.filter import (
        app,
        broker,
        default_handler,
        handle,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})
        default_handler.mock.assert_called_once_with("Hello, FastStream!")


@pytest.mark.asyncio()
@require_nats
async def test_nats_filtering() -> None:
    from docs.docs_src.getting_started.subscription.nats.filter import (
        app,
        broker,
        default_handler,
        handle,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})
        default_handler.mock.assert_called_once_with("Hello, FastStream!")


@pytest.mark.asyncio()
@require_redis
async def test_redis_filtering() -> None:
    from docs.docs_src.getting_started.subscription.redis.filter import (
        app,
        broker,
        default_handler,
        handle,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})
        default_handler.mock.assert_called_once_with("Hello, FastStream!")
