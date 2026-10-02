import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream.mqtt import MQTTRouter, QoS, Will
from faststream.mqtt.fastapi import MQTTRouter as StreamRouter
from tests.brokers.base.fastapi import FastAPILocalTestcase, FastAPITestcase

from .basic import MQTTMemoryTestcaseConfig, MQTTTestcaseConfig
from .settings import Settings


@pytest.mark.mqtt()
@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "options",
    (
        {"receive_maximum": 10},
        {"maximum_packet_size": 4096},
        {"user_properties": (("role", "worker"),)},
        {"request_response_information": False},
        {"request_problem_information": False},
    ),
)
async def test_router_connect_properties_reject_v311(options: dict[str, Any]) -> None:
    router = StreamRouter(version="3.1.1", **options)
    with pytest.raises(RuntimeError, match=r"MQTT 5\.0 is required"):
        await router.broker.connect()


class MQTTFastAPITestcaseConfig(MQTTTestcaseConfig):
    @pytest.fixture(autouse=True)
    def setup_version(self) -> None:
        self.version = "5.0"


class MQTTFastAPIMemoryTestcaseConfig(MQTTMemoryTestcaseConfig):
    @pytest.fixture(autouse=True)
    def setup_version(self) -> None:
        self.version = "5.0"


@pytest.mark.mqtt()
def test_router_url() -> None:
    router = StreamRouter("mqtts://router:8884")

    assert router.broker._connection_kwargs["host"] == "router"
    assert router.broker._connection_kwargs["port"] == 8884
    assert router.broker._connection_kwargs["tls"] is True


@pytest.mark.mqtt()
def test_router_will_threaded_to_broker() -> None:
    will = Will(
        topic="status/service",
        payload=b"offline",
        qos=QoS.AT_LEAST_ONCE,
        retain=True,
    )

    router = StreamRouter(will=will)

    assert router.broker._connection_kwargs["will"] is will


@pytest.mark.mqtt()
def test_router_recovery_callback_threaded_to_broker() -> None:
    async def on_connection_recovery_failed() -> None:
        pass

    router = StreamRouter(
        on_connection_recovery_failed=on_connection_recovery_failed,
    )

    assert (
        router.broker._connection_kwargs["on_connection_recovery_failed"]
        is on_connection_recovery_failed
    )


@pytest.mark.mqtt()
def test_router_session_replay_config_threaded_to_broker() -> None:
    router = StreamRouter(
        session_replay_buffer_size=10,
        session_replay_timeout=20.0,
    )

    assert router.broker._connection_kwargs["session_replay_buffer_size"] == 10
    assert router.broker._connection_kwargs["session_replay_timeout"] == 20.0


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestRouter(MQTTFastAPITestcaseConfig, FastAPITestcase):
    router_class = StreamRouter
    broker_router_class = MQTTRouter

    async def test_connect_properties(self, settings: Settings, queue: str) -> None:
        router = self.router_class(
            host=settings.host,
            port=settings.port,
            version="5.0",
            receive_maximum=10,
            maximum_packet_size=4096,
            user_properties=(("role", "worker"), ("role", "reader")),
            request_response_information=True,
            request_problem_information=False,
        )
        subscriber = router.subscriber(queue, qos=QoS.AT_LEAST_ONCE)
        publisher = router.publisher(queue, qos=QoS.AT_LEAST_ONCE)

        async with router.broker:
            await router.broker.start()
            assert await subscriber.get_one(timeout=0.01) is None
            await publisher.publish("accepted")
            message = await subscriber.get_one()

        assert message is not None
        assert await message.decode() == "accepted"

    async def test_path(self, queue: str, mock: MagicMock, event: asyncio.Event) -> None:
        router = self.router_class()

        @router.subscriber(queue + "/{name}")
        def subscriber(msg: str, name: str) -> None:
            mock(msg=msg, name=name)
            event.set()

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(
                        router.broker.publish("hello", f"{queue}/john"),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(msg="hello", name="john")


@pytest.mark.mqtt()
class TestRouterLocal(MQTTFastAPIMemoryTestcaseConfig, FastAPILocalTestcase):
    router_class = StreamRouter
    broker_router_class = MQTTRouter

    async def test_path(self, queue: str) -> None:
        router = self.router_class()

        @router.subscriber(queue + "/{name}")
        async def hello(name: str) -> str:
            return name

        async with self.patch_broker(router.broker) as br:
            r = await br.request(
                "hi",
                f"{queue}/john",
                timeout=0.5,
            )
            assert await r.decode() == "john"
