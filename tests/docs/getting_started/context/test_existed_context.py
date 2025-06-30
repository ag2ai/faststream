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
async def test_existed_context_kafka() -> None:
    from docs.docs_src.getting_started.context.kafka.existed_context import (
        broker_object,
    )
    from faststream.kafka import TestKafkaBroker

    @broker_object.subscriber("response")
    async def resp() -> None: ...

    async with TestKafkaBroker(broker_object) as br:
        await br.publish("Hi!", "test-topic")
        await br.publish("Hi!", "response-topic")

        assert resp.mock.call_count == 2


@pytest.mark.asyncio()
@require_confluent
async def test_existed_context_confluent() -> None:
    from docs.docs_src.getting_started.context.confluent.existed_context import (
        broker_object,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    @broker_object.subscriber("response")
    async def resp() -> None: ...

    async with TestConfluentKafkaBroker(broker_object) as br:
        await br.publish("Hi!", "test-topic")
        await br.publish("Hi!", "response-topic")

        assert resp.mock.call_count == 2


@pytest.mark.asyncio()
@require_aiopika
async def test_existed_context_rabbit() -> None:
    from docs.docs_src.getting_started.context.rabbit.existed_context import (
        broker_object,
    )
    from faststream.rabbit import TestRabbitBroker

    @broker_object.subscriber("response")
    async def resp() -> None: ...

    async with TestRabbitBroker(broker_object) as br:
        await br.publish("Hi!", "test-queue")
        await br.publish("Hi!", "response-queue")

        assert resp.mock.call_count == 2


@pytest.mark.asyncio()
@require_nats
async def test_existed_context_nats() -> None:
    from docs.docs_src.getting_started.context.nats.existed_context import (
        broker_object,
    )
    from faststream.nats import TestNatsBroker

    @broker_object.subscriber("response")
    async def resp() -> None: ...

    async with TestNatsBroker(broker_object) as br:
        await br.publish("Hi!", "test-subject")
        await br.publish("Hi!", "response-subject")

        assert resp.mock.call_count == 2


@pytest.mark.asyncio()
@require_redis
async def test_existed_context_redis() -> None:
    from docs.docs_src.getting_started.context.redis.existed_context import (
        broker_object,
    )
    from faststream.redis import TestRedisBroker

    @broker_object.subscriber("response")
    async def resp() -> None: ...

    async with TestRedisBroker(broker_object) as br:
        await br.publish("Hi!", "test-channel")
        await br.publish("Hi!", "response-channel")

        assert resp.mock.call_count == 2
