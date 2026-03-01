import asyncio
from unittest.mock import MagicMock

import pytest

from faststream import Path
from faststream.redis import (
    RedisPublisher,
    RedisRoute,
    RedisRouter,
)
from tests.brokers.base.router import RouterLocalTestcase, RouterTestcase

from .basic import RedisMemoryTestcaseConfig, RedisTestcaseConfig


@pytest.mark.connected()
@pytest.mark.redis()
class TestRouter(RedisTestcaseConfig, RouterTestcase):
    route_class = RedisRoute
    publisher_class = RedisPublisher


@pytest.mark.redis()
@pytest.mark.connected()
class TestRouterLocal(RedisMemoryTestcaseConfig, RouterLocalTestcase):
    route_class = RedisRoute
    publisher_class = RedisPublisher

    async def test_router_path(self, event: asyncio.Event, mock: MagicMock) -> None:
        pub_broker = self.get_broker(apply_types=True)

        router = RedisRouter()

        @router.subscriber("in.{name}.{id}")
        async def h(
            name: str = Path(),
            id: int = Path("id"),
        ) -> None:
            event.set()
            mock(name=name, id=id)

        pub_broker.include_router(router)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await br.request("", "in.john.2")

            assert event.is_set()
            mock.assert_called_once_with(name="john", id=2)

    async def test_router_path_with_prefix(
        self,
        event: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        router = RedisRouter(prefix="test.")

        @router.subscriber("in.{name}.{id}")
        async def h(
            name: str = Path(),
            id: int = Path("id"),
        ) -> None:
            event.set()
            mock(name=name, id=id)

        pub_broker.include_router(router)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await br.request("", "test.in.john.2")

            assert event.is_set()
            mock.assert_called_once_with(name="john", id=2)

    async def test_router_delay_handler_path(
        self,
        event: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        async def h(
            name: str = Path(),
            id: int = Path("id"),
        ) -> None:
            event.set()
            mock(name=name, id=id)

        r = RedisRouter(handlers=(self.route_class(h, channel="in.{name}.{id}"),))

        pub_broker.include_router(r)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await br.request("", "in.john.2")

            assert event.is_set()
            mock.assert_called_once_with(name="john", id=2)

    async def test_delayed_channel_handlers(
        self, queue: str, event: asyncio.Event
    ) -> None:
        pub_broker = self.get_broker()

        def response(m) -> None:
            event.set()

        r = RedisRouter(prefix="test_", handlers=(RedisRoute(response, channel=queue),))

        pub_broker.include_router(r)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", channel=f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()

    async def test_delayed_list_handlers(self, queue: str, event: asyncio.Event) -> None:
        pub_broker = self.get_broker()

        def response(m) -> None:
            event.set()

        r = RedisRouter(prefix="test_", handlers=(RedisRoute(response, list=queue),))

        pub_broker.include_router(r)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", list=f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )
            assert event.is_set()

    async def test_delayed_stream_handlers(
        self, queue: str, event: asyncio.Event
    ) -> None:
        pub_broker = self.get_broker()

        def response(m) -> None:
            event.set()

        r = RedisRouter(prefix="test_", handlers=(RedisRoute(response, stream=queue),))

        pub_broker.include_router(r)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", stream=f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()
