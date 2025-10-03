import asyncio
from typing import Any

import pytest

from faststream._internal.configs.settings import Settings, SettingsContainer
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue


@pytest.mark.asyncio()
@pytest.mark.rabbit()
@pytest.mark.connected()
async def test_queue_from_settings(event: asyncio.Event, queue: str) -> None:
    broker = RabbitBroker(settings=SettingsContainer(q1=queue))

    @broker.subscriber(queue=Settings("q1"))
    def h(m: Any) -> None:
        event.set()

    publisher = broker.publisher(queue=Settings("q1"))

    async with broker:
        await broker.start()

        await asyncio.wait(
            (
                asyncio.create_task(publisher.publish("test")),
                asyncio.create_task(event.wait()),
            ),
            timeout=3,
        )

    assert event.is_set()


@pytest.mark.asyncio()
@pytest.mark.rabbit()
@pytest.mark.connected()
async def test_queue_object_name_from_settings(
    event: asyncio.Event,
    queue: str,
) -> None:
    broker = RabbitBroker(settings=SettingsContainer(queue_name=queue))

    @broker.subscriber(queue=RabbitQueue(Settings("queue_name")))
    def h(m: Any) -> None:
        event.set()

    publisher = broker.publisher(queue=RabbitQueue(Settings("queue_name")))

    async with broker:
        await broker.start()

        await asyncio.wait(
            (
                asyncio.create_task(publisher.publish("test")),
                asyncio.create_task(event.wait()),
            ),
            timeout=3,
        )

    assert event.is_set()


@pytest.mark.asyncio()
@pytest.mark.rabbit()
@pytest.mark.connected()
async def test_nested_settings(
    event: asyncio.Event,
    queue: str,
) -> None:
    settings = SettingsContainer(
        ex=RabbitExchange(f"{queue}2"),
        rk=queue,
    )

    broker = RabbitBroker(settings=settings)

    @broker.subscriber(
        queue=RabbitQueue(name=f"{queue}1", routing_key=Settings("rk")),
        exchange=Settings("ex"),
    )
    def h(m: Any) -> None:
        event.set()

    publisher = broker.publisher(
        queue=RabbitQueue(name=f"{queue}1", routing_key=Settings("rk")),
        exchange=Settings("ex"),
        routing_key=Settings("rk"),
    )

    async with broker:
        await broker.start()

        await asyncio.wait(
            (
                asyncio.create_task(publisher.publish("test")),
                asyncio.create_task(event.wait()),
            ),
            timeout=3,
        )

    assert event.is_set()
