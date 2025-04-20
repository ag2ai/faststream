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
async def test_index_kafka_base() -> None:
    from docs.docs_src.index.kafka.basic import broker, handle_msg
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker) as br:
        await br.publish({"user": "John", "user_id": 1}, "in-topic")

        handle_msg.mock.assert_called_once_with({"user": "John", "user_id": 1})

        list(br._publishers)[0].mock.assert_called_once_with(  # noqa: RUF015
            "User: 1 - John registered",
        )


@pytest.mark.asyncio()
@require_confluent
async def test_index_confluent_base() -> None:
    from docs.docs_src.index.confluent.basic import broker, handle_msg
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker) as br:
        await br.publish({"user": "John", "user_id": 1}, "in-topic")

        handle_msg.mock.assert_called_once_with({"user": "John", "user_id": 1})

        list(br._publishers)[0].mock.assert_called_once_with(  # noqa: RUF015
            "User: 1 - John registered",
        )


@pytest.mark.asyncio()
@require_aiopika
async def test_index_rabbit_base() -> None:
    from docs.docs_src.index.rabbit.basic import broker, handle_msg
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker) as br:
        await br.publish({"user": "John", "user_id": 1}, "in-queue")

        handle_msg.mock.assert_called_once_with({"user": "John", "user_id": 1})

        list(br._publishers)[0].mock.assert_called_once_with(  # noqa: RUF015
            "User: 1 - John registered",
        )


@pytest.mark.asyncio()
@require_nats
async def test_index_nats_base() -> None:
    from docs.docs_src.index.nats.basic import broker, handle_msg
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker) as br:
        await br.publish({"user": "John", "user_id": 1}, "in-subject")

        handle_msg.mock.assert_called_once_with({"user": "John", "user_id": 1})

        list(br._publishers)[0].mock.assert_called_once_with(  # noqa: RUF015
            "User: 1 - John registered",
        )


@pytest.mark.asyncio()
@require_redis
async def test_index_redis_base() -> None:
    from docs.docs_src.index.redis.basic import broker, handle_msg
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker) as br:
        await br.publish({"user": "John", "user_id": 1}, "in-channel")

        handle_msg.mock.assert_called_once_with({"user": "John", "user_id": 1})

        list(br._publishers)[0].mock.assert_called_once_with(  # noqa: RUF015
            "User: 1 - John registered",
        )
