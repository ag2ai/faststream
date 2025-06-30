import asyncio
from unittest.mock import MagicMock

import pytest

from faststream.nats import JStream, NatsRouter, PullSub
from faststream.nats.fastapi import NatsRouter as StreamRouter
from tests.brokers.base.fastapi import FastAPILocalTestcase, FastAPITestcase

from .basic import NatsMemoryTestcaseConfig, NatsTestcaseConfig


@pytest.mark.nats()
class TestRouter(NatsTestcaseConfig, FastAPITestcase):
    router_class = StreamRouter
    broker_router_class = NatsRouter

    async def test_path(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        router = self.router_class()

        @router.subscriber(queue + ".{name}")
        def subscriber(msg: str, name: str) -> None:
            mock(msg=msg, name=name)
            event.set()

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(
                        router.broker.publish("hello", f"{queue}.john"),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(msg="hello", name="john")

    async def test_consume_batch(
        self,
        queue: str,
        stream: JStream,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        router = self.router_class()

        @router.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True),
        )
        def subscriber(m: list[str]) -> None:
            mock(m)
            event.set()

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish(b"hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(["hello"])


class TestRouterLocal(NatsMemoryTestcaseConfig, FastAPILocalTestcase):
    router_class = StreamRouter
    broker_router_class = NatsRouter

    async def test_consume_batch(
        self,
        queue: str,
        stream: JStream,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        router = self.router_class()

        @router.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True),
        )
        def subscriber(m: list[str]) -> None:
            mock(m)
            event.set()

        async with self.patch_broker(router.broker) as br:
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(b"hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(["hello"])

    async def test_path(self, queue: str) -> None:
        router = self.router_class()

        @router.subscriber(queue + ".{name}")
        async def hello(name):
            return name

        async with self.patch_broker(router.broker) as br:
            r = await br.request(
                "hi",
                f"{queue}.john",
                timeout=0.5,
            )
            assert await r.decode() == "john"
