import asyncio
from functools import partial

import pytest

from faststream.mq.broker.router import MQRouter as BrokerRouter
from faststream.mq.fastapi import MQRouter as StreamRouter
from tests.brokers.base.fastapi import FastAPILocalTestcase

from .basic import MQMemoryTestcaseConfig, MQTestcaseConfig


@pytest.mark.connected()
@pytest.mark.mq()
class TestRouter(MQTestcaseConfig):
    @pytest.mark.asyncio()
    async def test_path(self, queue: str, mock) -> None:
        event = asyncio.Event()

        router = StreamRouter(
            queue_manager="QM1",
            conn_name="127.0.0.1(1414)",
            username="app",
            password="password",
        )

        @router.subscriber(queue)
        async def subscriber(msg: str) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(router.broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with("hello")


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestRouterLocal(MQMemoryTestcaseConfig, FastAPILocalTestcase):
    router_class = partial(StreamRouter, queue_manager="QM1")
    broker_router_class = BrokerRouter
