import asyncio
from unittest.mock import MagicMock

import pytest

from faststream.mqtt import MQTTRouter
from faststream.mqtt.fastapi import MQTTRouter as StreamRouter
from tests.brokers.base.fastapi import FastAPILocalTestcase, FastAPITestcase

from .basic import MQTTMemoryTestcaseConfig, MQTTTestcaseConfig


class MQTTFastAPITestcaseConfig(MQTTTestcaseConfig):
    @pytest.fixture(autouse=True)
    def setup_version(self) -> None:
        self.version = "5.0"


class MQTTFastAPIMemoryTestcaseConfig(MQTTMemoryTestcaseConfig):
    @pytest.fixture(autouse=True)
    def setup_version(self) -> None:
        self.version = "5.0"


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestRouter(MQTTFastAPITestcaseConfig, FastAPITestcase):
    router_class = StreamRouter
    broker_router_class = MQTTRouter

    async def test_path(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

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
