import asyncio
from typing import List
from unittest.mock import Mock

import pytest

from faststream.redis import ListSub, RedisRouter, StreamSub
from faststream.redis.fastapi import RedisRouter as StreamRouter
from faststream.redis.testing import TestRedisBroker, build_message
from tests.brokers.base.fastapi import FastAPILocalTestcase, FastAPITestcase


@pytest.mark.redis
class TestRouter(FastAPITestcase):
    router_class = StreamRouter
    broker_router_class = RedisRouter

    async def test_path(
        self,
        queue: str,
        event: asyncio.Event,
        mock: Mock,
    ):
        router = self.router_class()

        @router.subscriber("in.{name}")
        def subscriber(msg: str, name: str):
            mock(msg=msg, name=name)
            event.set()

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hello", "in.john")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(msg="hello", name="john")

    async def test_connection_params(self, settings):
        broker = self.router_class(
            host="fake-host", port=6377
        ).broker  # kwargs will be ignored
        await broker.connect(
            host=settings.host,
            port=settings.port,
        )
        await broker._connection.ping()
        await broker.stop()

    async def test_batch_real(
        self,
        mock: Mock,
        queue: str,
        event: asyncio.Event,
    ):
        router = self.router_class()

        @router.subscriber(list=ListSub(queue, batch=True, max_records=1))
        async def hello(msg: List[str]):
            event.set()
            return mock(msg)

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hi", list=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])

    @pytest.mark.slow
    async def test_consume_stream(
        self,
        event: asyncio.Event,
        mock: Mock,
        queue,
    ):
        router = self.router_class()

        @router.subscriber(stream=StreamSub(queue, polling_interval=1000))
        async def handler(msg):
            mock(msg)
            event.set()

        async with router.broker:
            await router.broker.start()
            await asyncio.sleep(0.5)

            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hello", stream=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with("hello")

    @pytest.mark.slow
    async def test_consume_stream_batch(
        self,
        event: asyncio.Event,
        mock: Mock,
        queue,
    ):
        router = self.router_class()

        @router.subscriber(stream=StreamSub(queue, polling_interval=1000, batch=True))
        async def handler(msg: List[str]):
            mock(msg)
            event.set()

        async with router.broker:
            await router.broker.start()
            await asyncio.sleep(0.5)

            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hello", stream=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with(["hello"])


class TestRouterLocal(FastAPILocalTestcase):
    router_class = StreamRouter
    broker_router_class = RedisRouter
    broker_test = staticmethod(TestRedisBroker)
    build_message = staticmethod(build_message)

    async def test_batch_testclient(
        self,
        mock: Mock,
        queue: str,
        event: asyncio.Event,
    ):
        router = self.router_class()

        @router.subscriber(list=ListSub(queue, batch=True, max_records=1))
        async def hello(msg: List[str]):
            event.set()
            return mock(msg)

        async with TestRedisBroker(router.broker):
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hi", list=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])

    async def test_stream_batch_testclient(
        self,
        mock: Mock,
        queue: str,
        event: asyncio.Event,
    ):
        router = self.router_class()

        @router.subscriber(stream=StreamSub(queue, batch=True))
        async def hello(msg: List[str]):
            event.set()
            return mock(msg)

        async with TestRedisBroker(router.broker):
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hi", stream=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])

    async def test_path(self, queue: str):
        router = self.router_class()

        @router.subscriber(queue + ".{name}")
        async def hello(name):
            return name

        async with self.broker_test(router.broker):
            r = await router.broker.publish(
                "hi",
                f"{queue}.john",
                rpc=True,
                rpc_timeout=0.5,
            )
            assert r == "john"
