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
async def test_decorator_kafka() -> None:
    from docs.docs_src.getting_started.publishing.kafka.decorator import (
        app,
        broker,
        handle,
        handle_next,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")
        handle_next.mock.assert_called_once_with("Hi!")
        next(iter(broker._publishers)).mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_confluent
async def test_decorator_confluent() -> None:
    from docs.docs_src.getting_started.publishing.confluent.decorator import (
        app,
        broker,
        handle,
        handle_next,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")
        handle_next.mock.assert_called_once_with("Hi!")
        next(iter(broker._publishers)).mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_aiopika
async def test_decorator_rabbit() -> None:
    from docs.docs_src.getting_started.publishing.rabbit.decorator import (
        app,
        broker,
        handle,
        handle_next,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")
        handle_next.mock.assert_called_once_with("Hi!")
        next(iter(broker._publishers)).mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_nats
async def test_decorator_nats() -> None:
    from docs.docs_src.getting_started.publishing.nats.decorator import (
        app,
        broker,
        handle,
        handle_next,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")
        handle_next.mock.assert_called_once_with("Hi!")
        next(iter(broker._publishers)).mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_redis
async def test_decorator_redis() -> None:
    from docs.docs_src.getting_started.publishing.redis.decorator import (
        app,
        broker,
        handle,
        handle_next,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")
        handle_next.mock.assert_called_once_with("Hi!")
        next(iter(broker._publishers)).mock.assert_called_once_with("Hi!")
