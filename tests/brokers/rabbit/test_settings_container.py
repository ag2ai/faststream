import asyncio

import pytest

from faststream._internal.configs.settings import Settings, SettingsContainer
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue


@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_settings_container() -> None:
    event = asyncio.Event()
    q = RabbitQueue("test")

    settings = SettingsContainer(q1=q)
    broker = RabbitBroker(settings=settings)
    pub_ = broker.publisher(queue=Settings("q1"))

    @broker.subscriber(queue=Settings("q1"))
    def h(m) -> None:
        event.set()

    await broker.start()
    await pub_.publish("test")
    await broker.stop()
    assert event.is_set()


@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_settings_container1() -> None:
    event = asyncio.Event()
    settings = SettingsContainer(queue_name="test")
    broker = RabbitBroker(settings=settings)
    pub_ = broker.publisher(queue=RabbitQueue(Settings("queue_name")))

    @broker.subscriber(queue=RabbitQueue(Settings("queue_name")))
    def h(m) -> None:
        event.set()

    await broker.start()
    await pub_.publish("test")
    await broker.stop()
    assert event.is_set()


@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_nested_settings_container() -> None:
    event = asyncio.Event()
    ex = RabbitExchange("tt")

    settings = SettingsContainer(ex=ex, rk="rk")
    broker = RabbitBroker(settings=settings)
    pub_ = broker.publisher(
        queue=RabbitQueue(name="test", routing_key=Settings("rk")),
        exchange=Settings("ex"),
        routing_key=Settings("rk")
    )

    @broker.subscriber(
        queue=RabbitQueue(name="test", routing_key=Settings("rk")),
        exchange=Settings("ex")
    )
    def h(m) -> None:
        event.set()

    await broker.start()
    await pub_.publish("test")
    await broker.stop()
    assert event.is_set()
