import asyncio
from unittest.mock import MagicMock

import pytest

from faststream import Path
from faststream.rabbit import (
    ExchangeType,
    RabbitExchange,
    RabbitPublisher,
    RabbitQueue,
    RabbitRoute,
    RabbitRouter,
)
from tests.brokers.base.router import RouterLocalTestcase, RouterTestcase

from .basic import RabbitMemoryTestcaseConfig, RabbitTestcaseConfig


@pytest.mark.rabbit()
class TestRouter(RabbitTestcaseConfig, RouterTestcase):
    route_class = RabbitRoute
    publisher_class = RabbitPublisher

    async def test_router_path(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
        router: RabbitRouter,
    ) -> None:
        @router.subscriber(
            RabbitQueue(
                queue,
                routing_key="in.{name}.{id}",
            ),
            RabbitExchange(
                queue + "1",
                type=ExchangeType.TOPIC,
            ),
        )
        async def h(
            name: str = Path(),
            id: int = Path("id"),
        ) -> None:
            event.set()
            mock(name=name, id=id)

        pub_broker = self.get_broker(apply_types=True)
        pub_broker.include_router(router)

        await pub_broker.start()

        await pub_broker.request(
            "",
            "in.john.2",
            queue + "1",
        )

        assert event.is_set()
        mock.assert_called_once_with(name="john", id=2)

    async def test_router_delay_handler_path(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
        router: RabbitRouter,
    ) -> None:
        async def h(
            name: str = Path(),
            id: int = Path("id"),
        ) -> None:
            event.set()
            mock(name=name, id=id)

        r = type(router)(
            handlers=(
                self.route_class(
                    h,
                    queue=RabbitQueue(
                        queue,
                        routing_key="in.{name}.{id}",
                    ),
                    exchange=RabbitExchange(
                        queue + "1",
                        type=ExchangeType.TOPIC,
                    ),
                ),
            ),
        )

        pub_broker = self.get_broker(apply_types=True)
        pub_broker.include_router(r)

        await pub_broker.start()

        await pub_broker.request(
            "",
            "in.john.2",
            queue + "1",
        )

        assert event.is_set()
        mock.assert_called_once_with(name="john", id=2)

    async def test_queue_obj(
        self,
        router: RabbitRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        broker = self.get_broker()

        router.prefix = "test/"

        r_queue = RabbitQueue(queue)

        @router.subscriber(r_queue)
        def subscriber(m) -> None:
            event.set()

        broker.include_router(router)

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        broker.publish("hello", f"test/{r_queue.name}"),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()

    async def test_queue_obj_with_routing_key(
        self,
        router: RabbitRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        broker = self.get_broker()

        router.prefix = "test/"

        r_queue = RabbitQueue("useless", routing_key=f"{queue}1")
        exchange = RabbitExchange(f"{queue}exch")

        @router.subscriber(r_queue, exchange=exchange)
        def subscriber(m) -> None:
            event.set()

        broker.include_router(router)

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        broker.publish("hello", f"test/{queue}1", exchange=exchange),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()

    async def test_delayed_handlers_with_queue(
        self,
        router: RabbitRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        def response(m) -> None:
            event.set()

        r_queue = RabbitQueue(queue)

        r = type(router)(
            prefix="test/",
            handlers=(self.route_class(response, queue=r_queue),),
        )

        pub_broker = self.get_broker()
        pub_broker.include_router(r)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        pub_broker.publish("hello", f"test/{r_queue.name}"),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()


class TestRouterLocal(RabbitMemoryTestcaseConfig, RouterLocalTestcase):
    route_class = RabbitRoute
    publisher_class = RabbitPublisher
