import pytest

from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)


@pytest.mark.asyncio()
@require_aiopika
async def test_pydantic_model_rabbit() -> None:
    from docs.docs_src.getting_started.subscription.rabbit.pydantic_model import (
        broker,
        handle,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker) as br:
        await br.publish({"name": "John", "user_id": 1}, "test-queue")
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})


@pytest.mark.asyncio()
@require_aiokafka
async def test_pydantic_model_kafka() -> None:
    from docs.docs_src.getting_started.subscription.kafka.pydantic_model import (
        broker,
        handle,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker) as br:
        await br.publish({"name": "John", "user_id": 1}, "test-topic")
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})


@pytest.mark.asyncio()
@require_confluent
async def test_pydantic_model_confluent() -> None:
    from docs.docs_src.getting_started.subscription.confluent.pydantic_model import (
        broker,
        handle,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker) as br:
        await br.publish({"name": "John", "user_id": 1}, "test-topic")
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})


@pytest.mark.asyncio()
@require_nats
async def test_pydantic_model_nats() -> None:
    from docs.docs_src.getting_started.subscription.nats.pydantic_model import (
        broker,
        handle,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker) as br:
        await br.publish({"name": "John", "user_id": 1}, "test-subject")
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})


@pytest.mark.asyncio()
@require_redis
async def test_pydantic_model_redis() -> None:
    from docs.docs_src.getting_started.subscription.redis.pydantic_model import (
        broker,
        handle,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker) as br:
        await br.publish({"name": "John", "user_id": 1}, "test-channel")
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})
