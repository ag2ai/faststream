import pytest

from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_handle_kafka() -> None:
    from docs.docs_src.getting_started.subscription.kafka.advanced_testing import (
        test_handle,
        test_kafka_fields,
        test_message_context,
        test_several_messages,
    )

    await test_handle()
    await test_message_context()
    await test_several_messages()
    await test_kafka_fields()


@pytest.mark.confluent()
@pytest.mark.asyncio()
@require_confluent
async def test_handle_confluent() -> None:
    from docs.docs_src.getting_started.subscription.confluent.advanced_testing import (
        test_handle,
        test_kafka_fields,
        test_message_context,
        test_several_messages,
    )

    await test_handle()
    await test_message_context()
    await test_several_messages()
    await test_kafka_fields()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@require_aiopika
async def test_handle_rabbit() -> None:
    from docs.docs_src.getting_started.subscription.rabbit.advanced_testing import (
        test_handle,
        test_message_context,
        test_rabbit_fields,
        test_several_messages,
    )

    await test_handle()
    await test_message_context()
    await test_several_messages()
    await test_rabbit_fields()


@pytest.mark.nats()
@pytest.mark.asyncio()
@require_nats
async def test_handle_nats() -> None:
    from docs.docs_src.getting_started.subscription.nats.advanced_testing import (
        test_handle,
        test_message_context,
        test_nats_fields,
        test_several_messages,
    )

    await test_handle()
    await test_message_context()
    await test_several_messages()
    await test_nats_fields()


@pytest.mark.redis()
@pytest.mark.asyncio()
@require_redis
async def test_handle_redis() -> None:
    from docs.docs_src.getting_started.subscription.redis.advanced_testing import (
        test_handle,
        test_message_context,
        test_redis_fields,
        test_several_messages,
    )

    await test_handle()
    await test_message_context()
    await test_several_messages()
    await test_redis_fields()


@pytest.mark.mqtt()
@pytest.mark.asyncio()
@require_mqtt
async def test_handle_mqtt() -> None:
    from docs.docs_src.getting_started.subscription.mqtt.advanced_testing import (
        test_handle,
        test_message_context,
        test_mqtt_fields,
        test_several_messages,
    )

    await test_handle()
    await test_message_context()
    await test_several_messages()
    await test_mqtt_fields()
