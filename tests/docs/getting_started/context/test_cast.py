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
async def test_cast_kafka() -> None:
    from docs.docs_src.getting_started.context.kafka.cast import (
        broker,
        handle,
        handle_int,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

        handle.mock.assert_called_once_with("Hi!")

        await br.publish("Hi!", "test-topic2")

        handle_int.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_confluent
async def test_cast_confluent() -> None:
    from docs.docs_src.getting_started.context.confluent.cast import (
        broker,
        handle,
        handle_int,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker) as br:
        await br.publish("Hi!", "test-topic")

        handle.mock.assert_called_once_with("Hi!")

        await br.publish("Hi!", "test-topic2")

        handle_int.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_aiopika
async def test_cast_rabbit() -> None:
    from docs.docs_src.getting_started.context.rabbit.cast import (
        broker,
        handle,
        handle_int,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker) as br:
        await br.publish("Hi!", "test-queue")

        handle.mock.assert_called_once_with("Hi!")

        await br.publish("Hi!", "test-queue2")

        handle_int.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_nats
async def test_cast_nats() -> None:
    from docs.docs_src.getting_started.context.nats.cast import (
        broker,
        handle,
        handle_int,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker) as br:
        await br.publish("Hi!", "test-subject")

        handle.mock.assert_called_once_with("Hi!")

        await br.publish("Hi!", "test-subject2")

        handle_int.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_redis
async def test_cast_redis() -> None:
    from docs.docs_src.getting_started.context.redis.cast import (
        broker,
        handle,
        handle_int,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", "test-channel")

        handle.mock.assert_called_once_with("Hi!")

        await br.publish("Hi!", "test-channel2")

        handle_int.mock.assert_called_once_with("Hi!")
