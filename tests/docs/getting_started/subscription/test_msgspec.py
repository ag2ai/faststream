from typing import Any

import pytest

from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)

# the handlers assert the renamed `userId` field themselves, so a payload
# the serializer does not rename raises inside `publish`
PAYLOAD: dict[str, Any] = {"name": "John", "userId": 1}


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_msgspec_fields_kafka() -> None:
    from docs.docs_src.getting_started.subscription.kafka.msgspec_fields import (
        broker,
        handle,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker) as br:
        await br.publish(PAYLOAD, "test-channel")
        handle.mock.assert_called_once_with(PAYLOAD)


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_msgspec_struct_kafka() -> None:
    from docs.docs_src.getting_started.subscription.kafka.msgspec_struct import (
        broker,
        handle,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker) as br:
        await br.publish(PAYLOAD, "test-channel")
        handle.mock.assert_called_once_with(PAYLOAD)


@pytest.mark.confluent()
@pytest.mark.asyncio()
@require_confluent
async def test_msgspec_fields_confluent() -> None:
    from docs.docs_src.getting_started.subscription.confluent.msgspec_fields import (
        broker,
        handle,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker) as br:
        await br.publish(PAYLOAD, "test-channel")
        handle.mock.assert_called_once_with(PAYLOAD)


@pytest.mark.confluent()
@pytest.mark.asyncio()
@require_confluent
async def test_msgspec_struct_confluent() -> None:
    from docs.docs_src.getting_started.subscription.confluent.msgspec_struct import (
        broker,
        handle,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker) as br:
        await br.publish(PAYLOAD, "test-channel")
        handle.mock.assert_called_once_with(PAYLOAD)


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@require_aiopika
async def test_msgspec_fields_rabbit() -> None:
    from docs.docs_src.getting_started.subscription.rabbit.msgspec_fields import (
        broker,
        handle,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker) as br:
        await br.publish(PAYLOAD, "test-queue")
        handle.mock.assert_called_once_with(PAYLOAD)


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@require_aiopika
async def test_msgspec_struct_rabbit() -> None:
    from docs.docs_src.getting_started.subscription.rabbit.msgspec_struct import (
        broker,
        handle,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker) as br:
        await br.publish(PAYLOAD, "test-queue")
        handle.mock.assert_called_once_with(PAYLOAD)


@pytest.mark.nats()
@pytest.mark.asyncio()
@require_nats
async def test_msgspec_fields_nats() -> None:
    from docs.docs_src.getting_started.subscription.nats.msgspec_fields import (
        broker,
        handle,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker) as br:
        await br.publish(PAYLOAD, "test-subject")
        handle.mock.assert_called_once_with(PAYLOAD)


@pytest.mark.nats()
@pytest.mark.asyncio()
@require_nats
async def test_msgspec_struct_nats() -> None:
    from docs.docs_src.getting_started.subscription.nats.msgspec_struct import (
        broker,
        handle,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker) as br:
        await br.publish(PAYLOAD, "test-subject")
        handle.mock.assert_called_once_with(PAYLOAD)


@pytest.mark.redis()
@pytest.mark.asyncio()
@require_redis
async def test_msgspec_fields_redis() -> None:
    from docs.docs_src.getting_started.subscription.redis.msgspec_fields import (
        broker,
        handle,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker) as br:
        await br.publish(PAYLOAD, "test-channel")
        handle.mock.assert_called_once_with(PAYLOAD)


@pytest.mark.redis()
@pytest.mark.asyncio()
@require_redis
async def test_msgspec_struct_redis() -> None:
    from docs.docs_src.getting_started.subscription.redis.msgspec_struct import (
        broker,
        handle,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker) as br:
        await br.publish(PAYLOAD, "test-channel")
        handle.mock.assert_called_once_with(PAYLOAD)


@pytest.mark.mqtt()
@pytest.mark.asyncio()
@require_mqtt
async def test_msgspec_fields_mqtt() -> None:
    from docs.docs_src.getting_started.subscription.mqtt.msgspec_fields import (
        broker,
        handle,
    )
    from faststream.mqtt import TestMQTTBroker

    async with TestMQTTBroker(broker) as br:
        await br.publish(PAYLOAD, "test-topic")
        handle.mock.assert_called_once_with(PAYLOAD)


@pytest.mark.mqtt()
@pytest.mark.asyncio()
@require_mqtt
async def test_msgspec_struct_mqtt() -> None:
    from docs.docs_src.getting_started.subscription.mqtt.msgspec_struct import (
        broker,
        handle,
    )
    from faststream.mqtt import TestMQTTBroker

    async with TestMQTTBroker(broker) as br:
        await br.publish(PAYLOAD, "test-topic")
        handle.mock.assert_called_once_with(PAYLOAD)
