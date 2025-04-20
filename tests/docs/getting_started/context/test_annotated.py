import pytest

from tests.marks import (
    python39,
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)


@python39
@pytest.mark.asyncio()
@require_aiokafka
async def test_annotated_kafka() -> None:
    from docs.docs_src.getting_started.context.kafka.annotated import (
        base_handler,
        broker,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker) as br:
        await br.publish("Hi!", "test")

        base_handler.mock.assert_called_once_with("Hi!")


@python39
@pytest.mark.asyncio()
@require_confluent
async def test_annotated_confluent() -> None:
    from docs.docs_src.getting_started.context.confluent.annotated import (
        base_handler,
        broker,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker) as br:
        await br.publish("Hi!", "test")

        base_handler.mock.assert_called_once_with("Hi!")


@python39
@pytest.mark.asyncio()
@require_aiopika
async def test_annotated_rabbit() -> None:
    from docs.docs_src.getting_started.context.rabbit.annotated import (
        base_handler,
        broker,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker) as br:
        await br.publish("Hi!", "test")

        base_handler.mock.assert_called_once_with("Hi!")


@python39
@pytest.mark.asyncio()
@require_nats
async def test_annotated_nats() -> None:
    from docs.docs_src.getting_started.context.nats.annotated import (
        base_handler,
        broker,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker) as br:
        await br.publish("Hi!", "test")

        base_handler.mock.assert_called_once_with("Hi!")


@python39
@pytest.mark.asyncio()
@require_redis
async def test_annotated_redis() -> None:
    from docs.docs_src.getting_started.context.redis.annotated import (
        base_handler,
        broker,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", "test")

        base_handler.mock.assert_called_once_with("Hi!")
