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
async def test_broker_kafka() -> None:
    from docs.docs_src.getting_started.publishing.kafka.broker import (
        app,
        broker,
        handle,
        handle_next,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")
        handle_next.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_confluent
async def test_broker_confluent() -> None:
    from docs.docs_src.getting_started.publishing.confluent.broker import (
        app,
        broker,
        handle,
        handle_next,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")
        handle_next.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_aiopika
async def test_broker_rabbit() -> None:
    from docs.docs_src.getting_started.publishing.rabbit.broker import (
        app,
        broker,
        handle,
        handle_next,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")
        handle_next.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_nats
async def test_broker_nats() -> None:
    from docs.docs_src.getting_started.publishing.nats.broker import (
        app,
        broker,
        handle,
        handle_next,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")
        handle_next.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_redis
async def test_broker_redis() -> None:
    from docs.docs_src.getting_started.publishing.redis.broker import (
        app,
        broker,
        handle,
        handle_next,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")
        handle_next.mock.assert_called_once_with("Hi!")
