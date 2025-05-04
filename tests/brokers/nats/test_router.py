import asyncio

import pytest

from faststream import Path
from faststream.nats import (
    JStream,
    NatsPublisher,
    NatsRoute,
    NatsRouter,
)
from tests.brokers.base.router import RouterLocalTestcase, RouterTestcase

from .basic import NatsMemoryTestcaseConfig, NatsTestcaseConfig


@pytest.mark.nats()
class TestRouter(NatsTestcaseConfig, RouterTestcase):
    route_class = NatsRoute
    publisher_class = NatsPublisher

    async def test_router_path(
        self,
        event,
        mock,
        router: NatsRouter,
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        @router.subscriber("in.{name}.{id}")
        async def h(
            name: str = Path(),
            id: int = Path("id"),
        ) -> None:
            event.set()
            mock(name=name, id=id)

        pub_broker.include_router(router)

        await pub_broker.start()

        await pub_broker.request("", "in.john.2")

        assert event.is_set()
        mock.assert_called_once_with(name="john", id=2)

    async def test_path_as_first_with_prefix(
        self,
        event,
        mock,
        router: NatsRouter,
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        router.prefix = "root."

        @router.subscriber("{name}.nested")
        async def h(name: str = Path()) -> None:
            event.set()
            mock(name=name)

        pub_broker.include_router(router)

        await pub_broker.start()

        await pub_broker.request("", "root.john.nested")

        assert event.is_set()
        mock.assert_called_once_with(name="john")

    async def test_router_path_with_prefix(
        self,
        event,
        mock,
        router: NatsRouter,
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        router.prefix = "test."

        @router.subscriber("in.{name}.{id}")
        async def h(
            name: str = Path(),
            id: int = Path("id"),
        ) -> None:
            event.set()
            mock(name=name, id=id)

        pub_broker.include_router(router)

        await pub_broker.start()

        await pub_broker.request("", "test.in.john.2")

        assert event.is_set()
        mock.assert_called_once_with(name="john", id=2)

    async def test_router_delay_handler_path(
        self,
        event,
        mock,
        router: NatsRouter,
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        async def h(
            name: str = Path(),
            id: int = Path("id"),
        ) -> None:
            event.set()
            mock(name=name, id=id)

        r = type(router)(handlers=(self.route_class(h, subject="in.{name}.{id}"),))

        pub_broker.include_router(r)

        await pub_broker.start()

        await pub_broker.request("", "in.john.2")

        assert event.is_set()
        mock.assert_called_once_with(name="john", id=2)

    async def test_delayed_handlers_with_queue(
        self,
        router: NatsRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        def response(m) -> None:
            event.set()

        r = type(router)(
            prefix="test.",
            handlers=(self.route_class(response, subject=queue),),
        )

        pub_broker.include_router(r)

        await pub_broker.start()

        await asyncio.wait(
            (
                asyncio.create_task(pub_broker.publish("hello", f"test.{queue}")),
                asyncio.create_task(event.wait()),
            ),
            timeout=3,
        )

        assert event.is_set()


class TestRouterLocal(NatsMemoryTestcaseConfig, RouterLocalTestcase):
    route_class = NatsRoute
    publisher_class = NatsPublisher

    async def test_include_stream(
        self,
        router: NatsRouter,
    ) -> None:
        pub_broker = self.get_broker()

        @router.subscriber("test", stream="stream")
        async def handler() -> None: ...

        pub_broker.include_router(router)

        assert next(iter(pub_broker._stream_builder.objects.keys())) == "stream"

    async def test_include_stream_with_subjects(self) -> None:
        stream = JStream("test-stream")

        sub_router = NatsRouter(prefix="client.")

        sub_router.subscriber("1", stream=stream)
        sub_router.subscriber("2", stream=stream)

        router = NatsRouter(prefix="user.")

        router.subscriber("registered", stream=stream)

        router.include_router(sub_router)

        broker = self.get_broker()
        broker.include_router(router)

        assert set(stream.subjects) == {
            "user.registered",
            "user.client.1",
            "user.client.2",
        }
