import pytest

from faststream.kafka import TestKafkaBroker
from faststream.nats import TestNatsBroker
from faststream.rabbit import TestRabbitBroker
from faststream.redis import TestRedisBroker


@pytest.mark.asyncio
async def test_existed_context_kafka():
    from docs.docs_src.getting_started.context.kafka.existed_context import (
        broker_object,
    )

    @broker_object.subscriber("response")
    async def resp():
        ...

    async with TestKafkaBroker(broker_object) as br:
        await br.publish("Hi!", "test-topic")
        await br.publish("Hi!", "response-topic")

        assert resp.mock.call_count == 2


@pytest.mark.asyncio
async def test_existed_context_rabbit():
    from docs.docs_src.getting_started.context.rabbit.existed_context import (
        broker_object,
    )

    @broker_object.subscriber("response")
    async def resp():
        ...

    async with TestRabbitBroker(broker_object) as br:
        await br.publish("Hi!", "test-queue")
        await br.publish("Hi!", "response-queue")

        assert resp.mock.call_count == 2


@pytest.mark.asyncio
async def test_existed_context_nats():
    from docs.docs_src.getting_started.context.nats.existed_context import (
        broker_object,
    )

    @broker_object.subscriber("response")
    async def resp():
        ...

    async with TestNatsBroker(broker_object) as br:
        await br.publish("Hi!", "test-subject")
        await br.publish("Hi!", "response-subject")

        assert resp.mock.call_count == 2


@pytest.mark.asyncio
async def test_existed_context_redis():
    from docs.docs_src.getting_started.context.redis.existed_context import (
        broker_object,
    )

    @broker_object.subscriber("response")
    async def resp():
        ...

    async with TestRedisBroker(broker_object) as br:
        await br.publish("Hi!", "test-channel")
        await br.publish("Hi!", "response-channel")

        assert resp.mock.call_count == 2
