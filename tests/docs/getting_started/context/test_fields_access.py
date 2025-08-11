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
async def test_fields_access_kafka() -> None:
    from docs.docs_src.getting_started.context.kafka.fields_access import (
        broker,
        handle,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic", headers={"user": "John"})

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_confluent
async def test_fields_access_confluent() -> None:
    from docs.docs_src.getting_started.context.confluent.fields_access import (
        broker,
        handle,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic", headers={"user": "John"})

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_aiopika
async def test_fields_access_rabbit() -> None:
    from docs.docs_src.getting_started.context.rabbit.fields_access import (
        broker,
        handle,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker) as br:
        await br.publish("Hi!", "test-queue", headers={"user": "John"})

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_nats
async def test_fields_access_nats() -> None:
    from docs.docs_src.getting_started.context.nats.fields_access import (
        broker,
        handle,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker) as br:
        await br.publish("Hi!", "test-subject", headers={"user": "John"})

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_redis
async def test_fields_access_redis() -> None:
    from docs.docs_src.getting_started.context.redis.fields_access import (
        broker,
        handle,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", "test-channel", headers={"user": "John"})

        handle.mock.assert_called_once_with("Hi!")
