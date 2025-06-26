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
@pytest.mark.kafka()
@require_aiokafka
async def test_broker_context_kafka() -> None:
    from docs.docs_src.getting_started.publishing.kafka.broker_context import (
        app,
        broker,
        handle,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker, with_real=True), TestApp(app):
        await handle.wait_call(3)
        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@pytest.mark.confluent()
@require_confluent
async def test_broker_context_confluent() -> None:
    from docs.docs_src.getting_started.publishing.confluent.broker_context import (
        app,
        broker,
        handle,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker, with_real=True), TestApp(app):
        await handle.wait_call(30)
        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@pytest.mark.nats()
@require_nats
async def test_broker_context_nats() -> None:
    from docs.docs_src.getting_started.publishing.nats.broker_context import (
        app,
        broker,
        handle,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker, with_real=True), TestApp(app):
        await handle.wait_call(3)
        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@pytest.mark.rabbit()
@require_aiopika
async def test_broker_context_rabbit() -> None:
    from docs.docs_src.getting_started.publishing.rabbit.broker_context import (
        app,
        broker,
        handle,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker, with_real=True), TestApp(app):
        await handle.wait_call(3)
        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@pytest.mark.redis()
@require_redis
async def test_broker_context_redis() -> None:
    from docs.docs_src.getting_started.publishing.redis.broker_context import (
        app,
        broker,
        handle,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker, with_real=True), TestApp(app):
        await handle.wait_call(3)
        handle.mock.assert_called_once_with("Hi!")
