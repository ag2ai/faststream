import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream import Context
from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from tests.brokers.base.middlewares import LocalMiddlewareTestcase
from tests.brokers.base.parser import LocalCustomParserTestcase


@pytest.mark.asyncio()
class RouterTestcase(
    LocalMiddlewareTestcase,
    LocalCustomParserTestcase,
):
    route_class: type[SubscriberRoute]
    publisher_class: type[ArgsContainer]

    def get_router(self, **kwargs: Any) -> BrokerRouter:
        raise NotImplementedError

    async def test_router_dynamic_objects(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        nested_router = self.get_router()
        router = self.get_router(routers=[nested_router])
        broker = self.get_broker(routers=[router])

        def subscriber(m) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            args, kwargs = self.get_subscriber_params(queue)
            sub = nested_router.subscriber(*args, **kwargs)
            sub(subscriber)
            await sub.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("Hi!", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_empty_prefix(self, queue: str) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()
        router = self.get_router()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        def subscriber(m) -> None:
            event.set()

        pub_broker.include_router(router)
        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_not_empty_prefix(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        router = self.get_router(prefix="test_")

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        def subscriber(m) -> None:
            event.set()

        pub_broker.include_router(router)
        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_include_with_prefix(self, queue: str) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()
        router = self.get_router()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        def subscriber(m) -> None:
            event.set()

        pub_broker.include_router(router, prefix="test_")
        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_empty_prefix_publisher(self, queue: str) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()
        router = self.get_router()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        @router.publisher(queue + "resp")
        def subscriber(m) -> str:
            return "hi"

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @router.subscriber(*args2, **kwargs2)
        def response(m) -> None:
            event.set()

        pub_broker.include_router(router)
        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_not_empty_prefix_publisher(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        router = self.get_router(prefix="test_")

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        @router.publisher(queue + "resp")
        def subscriber(m) -> str:
            return "hi"

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @router.subscriber(*args2, **kwargs2)
        def response(m) -> None:
            event.set()

        pub_broker.include_router(router)
        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_include_publisher_with_prefix(
        self, queue: str, event: asyncio.Event
    ) -> None:
        broker = self.get_broker()

        args2, kwargs2 = self.get_subscriber_params(f"test_{queue}")

        @broker.subscriber(*args2, **kwargs2)
        async def handler(m: Any) -> None:
            event.set()

        router = self.get_router()
        publisher = router.publisher(queue)
        broker.include_router(router, prefix="test_")

        async with broker:
            await broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(publisher.publish("hello")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_manual_publisher(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        router = self.get_router(prefix="test_")

        p = router.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def subscriber(m) -> None:
            await p.publish("resp")

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @router.subscriber(*args2, **kwargs2)
        def response(m) -> None:
            event.set()

        pub_broker.include_router(router)
        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_delayed_handlers(self, queue: str) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        def response(m) -> None:
            event.set()

        args, kwargs = self.get_subscriber_params(queue)

        router = self.get_router(
            prefix="test_",
            handlers=(self.route_class(response, *args, **kwargs),),
        )

        pub_broker.include_router(router)
        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_delayed_publishers(self, queue: str, mock: MagicMock) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        def response(m):
            return m

        args, kwargs = self.get_subscriber_params(queue)

        router = self.get_router(
            prefix="test_",
            handlers=(
                self.route_class(
                    response,
                    *args,
                    **kwargs,
                    publishers=(self.publisher_class(queue + "1"),),
                ),
            ),
        )

        pub_broker.include_router(router)

        args, kwargs = self.get_subscriber_params(f"test_{queue}1")

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

            mock.assert_called_once_with("hello")

    async def test_nested_routers_sub(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        core_router = self.get_router(prefix="test1_")
        router = self.get_router(prefix="test2_")

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        def subscriber(m) -> str:
            event.set()
            mock(m)
            return "hi"

        core_router.include_routers(router)
        pub_broker.include_routers(core_router)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", f"test1_test2_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            mock.assert_called_with("hello")

    async def test_nested_routers_pub(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        core_router = self.get_router(prefix="test1_")
        router = self.get_router(prefix="test2_")

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        @router.publisher(queue + "resp")
        def subscriber(m) -> str:
            return "hi"

        args2, kwargs2 = self.get_subscriber_params(
            "test1_" + "test2_" + queue + "resp",
        )

        @pub_broker.subscriber(*args2, **kwargs2)
        def response(m) -> None:
            event.set()

        core_router.include_routers(router)
        pub_broker.include_router(core_router)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", f"test1_test2_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_router_parser(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        async def parser(msg, original):
            mock.parser()
            return await original(msg)

        async def decoder(msg, original):
            mock.decoder()
            return await original(msg)

        router = self.get_router(parser=parser, decoder=decoder)

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        def subscriber(s) -> None:
            event.set()

        pub_broker.include_router(router)
        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            mock.parser.assert_called_once()
            mock.decoder.assert_called_once()

    async def test_router_parser_override(self, queue: str, mock: MagicMock) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        async def global_parser(msg, original):  # pragma: no cover
            mock()
            return await original(msg)

        async def global_decoder(msg, original):  # pragma: no cover
            mock()
            return await original(msg)

        async def parser(msg, original):
            mock.parser()
            return await original(msg)

        async def decoder(msg, original):
            mock.decoder()
            return await original(msg)

        router = self.get_router(
            parser=global_parser,
            decoder=global_decoder,
        )

        args, kwargs = self.get_subscriber_params(queue, parser=parser, decoder=decoder)

        @router.subscriber(*args, **kwargs)
        def subscriber(s) -> None:
            event.set()

        pub_broker.include_router(router)
        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            assert not mock.called
            mock.parser.assert_called_once()
            mock.decoder.assert_called_once()

    async def test_router_in_init(self, queue: str) -> None:
        event = asyncio.Event()

        args, kwargs = self.get_subscriber_params(queue)
        router = self.get_router()

        @router.subscriber(*args, **kwargs)
        def subscriber(m) -> None:
            event.set()

        pub_broker = self.get_broker(routers=[router])

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_include_passes_producer_to_router(self) -> None:
        pub_broker = self.get_broker()

        router1 = self.get_router()
        router2 = self.get_router()

        pub1 = router1.publisher("l3")
        pub2 = router2.publisher("l3")

        assert pub1._outer_config.producer is not pub2._outer_config.producer

        pub_broker.include_routers(router2, router1)

        assert pub1._outer_config.producer is pub2._outer_config.producer


@pytest.mark.asyncio()
class RouterLocalTestcase(RouterTestcase):
    async def test_publisher_mock(self, queue: str, event: asyncio.Event) -> None:
        pub_broker = self.get_broker()
        router = self.get_router()

        pub = router.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        @pub
        def subscriber(m) -> str:
            event.set()
            return "hi"

        pub_broker.include_router(router)
        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            pub.mock.assert_called_with("hi")

    async def test_subscriber_mock(self, queue: str) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()
        router = self.get_router()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        def subscriber(m) -> str:
            event.set()
            return "hi"

        pub_broker.include_router(router)
        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            subscriber.mock.assert_called_with("hello")

    async def test_manual_publisher_mock(self, queue: str) -> None:
        pub_broker = self.get_broker()
        router = self.get_router()

        publisher = router.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def m(m) -> None:
            await publisher.publish("response")

        pub_broker.include_router(router)
        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await br.publish("hello", queue)
            publisher.mock.assert_called_with("response")

    async def test_func_wrapped_correctly_on_include_in_different_broker(self) -> None:
        router = self.get_router()
        broker1 = self.get_broker()
        broker2 = self.get_broker()

        @router.subscriber("in-queue")
        async def handle_msg(broker=Context()) -> str:
            return "test"

        broker1.include_router(router)
        async with self.patch_broker(broker1) as br:
            await br.publish({}, "in-queue")

        broker2.include_router(router)
        async with self.patch_broker(broker2) as br:
            await br.publish({}, "in-queue")
