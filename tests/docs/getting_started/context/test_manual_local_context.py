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
async def test_manual_local_context_kafka() -> None:
    from docs.docs_src.getting_started.context.kafka.manual_local_context import (
        broker,
        handle,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_confluent
async def test_manual_local_context_confluent() -> None:
    from docs.docs_src.getting_started.context.confluent.manual_local_context import (
        broker,
        handle,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_aiopika
async def test_manual_local_context_rabbit() -> None:
    from docs.docs_src.getting_started.context.rabbit.manual_local_context import (
        broker,
        handle,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker) as br:
        await br.publish("Hi!", "test-queue")

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_nats
async def test_manual_local_context_nats() -> None:
    from docs.docs_src.getting_started.context.nats.manual_local_context import (
        broker,
        handle,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker) as br:
        await br.publish("Hi!", "test-subject")

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_redis
async def test_manual_local_context_redis() -> None:
    from docs.docs_src.getting_started.context.redis.manual_local_context import (
        broker,
        handle,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", "test-channel")

        handle.mock.assert_called_once_with("Hi!")
