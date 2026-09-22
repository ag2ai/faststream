from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from functools import partial
from typing import Any
from unittest.mock import patch

import anyio
import pytest

from faststream import BaseMiddleware
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_mqtt,
    require_nats,
    require_redis,
)

# TestBroker does not support dynamic subscribers, so these run against a real
# broker; every test publishes the same payload because the snippets share
# a hardcoded destination
PAYLOAD = "Hi!"


@asynccontextmanager
async def publishing(broker: Any, destination: str) -> AsyncGenerator[None, None]:
    """Publish to `destination` until the block exits.

    The snippets consume with their own fixed timeouts, so a single message can
    be sent before their subscriber is ready to see it.
    """

    async def publish_forever() -> None:
        while True:
            await broker.publish(PAYLOAD, destination)
            await anyio.sleep(0.25)

    async with broker, anyio.create_task_group() as tg:
        _ = tg.start_soon(publish_forever)
        yield
        tg.cancel_scope.cancel()


async def consumed_message(snippet: Any, broker_name: str, destination: str) -> Any:
    """Run the snippet's `main()` while messages arrive, and return what it got."""
    broker_class = getattr(snippet, broker_name)

    async with publishing(broker_class(), destination):
        return await snippet.main()


async def iterated_message(snippet: Any, broker_name: str, destination: str) -> Any:
    """Run the snippet until its `async for` body sees a message, and return it."""
    received: list[Any] = []
    consumed = anyio.Event()

    class Recorder(BaseMiddleware[Any, Any]):
        async def consume_scope(
            self,
            call_next: Callable[[Any], Awaitable[Any]],
            msg: Any,
        ) -> Any:
            received.append(msg)
            consumed.set()
            return await call_next(msg)

    broker_class = getattr(snippet, broker_name)

    # the snippet builds its own broker, so the middleware watching what it
    # consumes goes in through the class the snippet calls
    with patch.object(
        snippet,
        broker_name,
        partial(broker_class, middlewares=(Recorder,)),
    ):
        async with (
            publishing(broker_class(), destination),
            anyio.create_task_group() as tg,
        ):
            _ = tg.start_soon(snippet.main)

            with anyio.fail_after(15.0):
                await consumed.wait()

            tg.cancel_scope.cancel()

    return received[0]


@pytest.mark.connected()
@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_dynamic() -> None:
    from docs.docs_src.getting_started.subscription.kafka import dynamic

    message = await consumed_message(dynamic, "KafkaBroker", "test-topic")

    assert message is not None
    assert await message.decode() == PAYLOAD


@pytest.mark.connected()
@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_dynamic_iter() -> None:
    from docs.docs_src.getting_started.subscription.kafka import dynamic_iter

    message = await iterated_message(dynamic_iter, "KafkaBroker", "test-topic")

    assert await message.decode() == PAYLOAD


@pytest.mark.connected()
@pytest.mark.rabbit()
@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_dynamic() -> None:
    from docs.docs_src.getting_started.subscription.rabbit import dynamic

    message = await consumed_message(dynamic, "RabbitBroker", "test-queue")

    assert message is not None
    assert await message.decode() == PAYLOAD


@pytest.mark.connected()
@pytest.mark.rabbit()
@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_dynamic_iter() -> None:
    from docs.docs_src.getting_started.subscription.rabbit import dynamic_iter

    message = await iterated_message(dynamic_iter, "RabbitBroker", "test-queue")

    assert await message.decode() == PAYLOAD


@pytest.mark.connected()
@pytest.mark.nats()
@pytest.mark.asyncio()
@require_nats
async def test_nats_dynamic() -> None:
    from docs.docs_src.getting_started.subscription.nats import dynamic

    message = await consumed_message(dynamic, "NatsBroker", "test-subject")

    assert message is not None
    assert await message.decode() == PAYLOAD


@pytest.mark.connected()
@pytest.mark.nats()
@pytest.mark.asyncio()
@require_nats
async def test_nats_dynamic_iter() -> None:
    from docs.docs_src.getting_started.subscription.nats import dynamic_iter

    message = await iterated_message(dynamic_iter, "NatsBroker", "test-subject")

    assert await message.decode() == PAYLOAD


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
@require_redis
async def test_redis_dynamic() -> None:
    from docs.docs_src.getting_started.subscription.redis import dynamic

    message = await consumed_message(dynamic, "RedisBroker", "test-channel")

    assert message is not None
    assert await message.decode() == PAYLOAD


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
@require_redis
async def test_redis_dynamic_iter() -> None:
    from docs.docs_src.getting_started.subscription.redis import dynamic_iter

    message = await iterated_message(dynamic_iter, "RedisBroker", "test-channel")

    assert await message.decode() == PAYLOAD


@pytest.mark.connected()
@pytest.mark.mqtt()
@pytest.mark.asyncio()
@require_mqtt
async def test_mqtt_dynamic() -> None:
    from docs.docs_src.getting_started.subscription.mqtt import dynamic

    message = await consumed_message(dynamic, "MQTTBroker", "test-topic")

    assert message is not None
    assert await message.decode() == PAYLOAD


@pytest.mark.connected()
@pytest.mark.mqtt()
@pytest.mark.asyncio()
@require_mqtt
async def test_mqtt_dynamic_iter() -> None:
    from docs.docs_src.getting_started.subscription.mqtt import dynamic_iter

    message = await iterated_message(dynamic_iter, "MQTTBroker", "test-topic")

    assert await message.decode() == PAYLOAD
