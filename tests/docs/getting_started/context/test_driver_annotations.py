import pytest

from faststream import TestApp
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)


@pytest.mark.asyncio()
@require_aiokafka
async def test_driver_annotations_kafka() -> None:
    from docs.docs_src.getting_started.context.kafka.driver_annotations import (
        app,
        broker,
        handle,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker) as br, TestApp(app):
        await br.publish("Hi!", "test-topic")

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_confluent
async def test_driver_annotations_confluent() -> None:
    from docs.docs_src.getting_started.context.confluent.driver_annotations import (
        app,
        broker,
        handle,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker) as br, TestApp(app):
        await br.publish("Hi!", "test-topic")

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_aiopika
async def test_driver_annotations_rabbit() -> None:
    from docs.docs_src.getting_started.context.rabbit.driver_annotations import (
        app,
        broker,
        handle,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker) as br, TestApp(app):
        await br.publish("Hi!", "test-queue")

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_nats
async def test_driver_annotations_nats() -> None:
    from docs.docs_src.getting_started.context.nats.driver_annotations import (
        app,
        broker,
        handle,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker) as br, TestApp(app):
        await br.publish("Hi!", "test-subject")

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_redis
async def test_driver_annotations_redis() -> None:
    from docs.docs_src.getting_started.context.redis.driver_annotations import (
        app,
        broker,
        handle,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker) as br, TestApp(app):
        await br.publish("Hi!", "test-channel")

        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_mqtt
async def test_driver_annotations_mqtt() -> None:
    from docs.docs_src.getting_started.context.mqtt.driver_annotations import (
        app,
        broker,
        handle,
    )
    from faststream.mqtt import TestMQTTBroker

    async with TestMQTTBroker(broker) as br, TestApp(app):
        await br.publish("Hi!", "test-topic")

        handle.mock.assert_called_once_with("Hi!")
