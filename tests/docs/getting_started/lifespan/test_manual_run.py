import asyncio
from collections.abc import Callable
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


async def wait_started(broker: Any) -> None:
    # the fake broker exposes no "started" event, so poll the subscribers
    while not all(sub.running for sub in broker.subscribers):  # noqa: ASYNC110
        await asyncio.sleep(0)


async def serve_and_stop(
    serve: Callable[[asyncio.Event], Any],
    broker: Any,
    handle: Any,
    destination: str,
) -> None:
    stop = asyncio.Event()
    server = asyncio.create_task(serve(stop))

    # the task has not run yet: give `serve` a chance to start the app
    await asyncio.wait_for(wait_started(broker), timeout=1)

    await broker.publish("Hi!", destination)
    handle.mock.assert_called_once_with("Hi!")

    stop.set()
    await server


@pytest.mark.asyncio()
@require_aiokafka
async def test_manual_run_kafka() -> None:
    from docs.docs_src.getting_started.lifespan.kafka.manual_run import (
        broker,
        handle,
        serve,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker, connect_only=True):
        await serve_and_stop(serve, broker, handle, "test-topic")


@pytest.mark.asyncio()
@require_confluent
async def test_manual_run_confluent() -> None:
    from docs.docs_src.getting_started.lifespan.confluent.manual_run import (
        broker,
        handle,
        serve,
    )
    from faststream.confluent import TestKafkaBroker

    async with TestKafkaBroker(broker, connect_only=True):
        await serve_and_stop(serve, broker, handle, "test-topic")


@pytest.mark.asyncio()
@require_aiopika
async def test_manual_run_rabbit() -> None:
    from docs.docs_src.getting_started.lifespan.rabbit.manual_run import (
        broker,
        handle,
        serve,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker, connect_only=True):
        await serve_and_stop(serve, broker, handle, "test-queue")


@pytest.mark.asyncio()
@require_nats
async def test_manual_run_nats() -> None:
    from docs.docs_src.getting_started.lifespan.nats.manual_run import (
        broker,
        handle,
        serve,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker, connect_only=True):
        await serve_and_stop(serve, broker, handle, "test-subject")


@pytest.mark.asyncio()
@require_redis
async def test_manual_run_redis() -> None:
    from docs.docs_src.getting_started.lifespan.redis.manual_run import (
        broker,
        handle,
        serve,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker, connect_only=True):
        await serve_and_stop(serve, broker, handle, "test-channel")


@pytest.mark.asyncio()
@require_mqtt
async def test_manual_run_mqtt() -> None:
    from docs.docs_src.getting_started.lifespan.mqtt.manual_run import (
        broker,
        handle,
        serve,
    )
    from faststream.mqtt import TestMQTTBroker

    async with TestMQTTBroker(broker, connect_only=True):
        await serve_and_stop(serve, broker, handle, "test-topic")
