import asyncio
from unittest.mock import Mock

import pytest

from faststream import Depends
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

    async def test_empty_prefix(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

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
        router: BrokerRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        router.prefix = "test_"

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

    async def test_include_with_prefix(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

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

    async def test_empty_prefix_publisher(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

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
        router: BrokerRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        router.prefix = "test_"

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

    async def test_manual_publisher(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        router.prefix = "test_"

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

    async def test_delayed_handlers(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        def response(m) -> None:
            event.set()

        args, kwargs = self.get_subscriber_params(queue)

        router = type(router)(
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

    async def test_delayed_publishers(
        self,
        router: BrokerRouter,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        def response(m):
            return m

        args, kwargs = self.get_subscriber_params(queue)

        r = type(router)(
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

        pub_broker.include_router(r)

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
        router: BrokerRouter,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        core_router = type(router)(prefix="test1_")
        router.prefix = "test2_"

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
        router: BrokerRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        core_router = type(router)(prefix="test1_")
        router.prefix = "test2_"

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

    async def test_router_dependencies(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        pub_broker = self.get_broker()

        router = type(router)(dependencies=(Depends(lambda: 1),))
        router2 = type(router)(dependencies=(Depends(lambda: 2),))

        args, kwargs = self.get_subscriber_params(
            queue,
            dependencies=(Depends(lambda: 3),),
        )

        @router2.subscriber(*args, **kwargs)
        def subscriber() -> None: ...

        router.include_router(router2)
        pub_broker.include_routers(router)

        sub = next(iter(pub_broker._subscribers))
        assert len((*sub._broker_dependencies, *sub.calls[0].dependencies)) == 3

    async def test_router_include_with_dependencies(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        pub_broker = self.get_broker()

        router2 = type(router)()

        args, kwargs = self.get_subscriber_params(
            queue,
            dependencies=(Depends(lambda: 3),),
        )

        @router2.subscriber(*args, **kwargs)
        def subscriber() -> None: ...

        router.include_router(router2, dependencies=(Depends(lambda: 2),))
        pub_broker.include_router(router, dependencies=(Depends(lambda: 1),))

        sub = next(iter(pub_broker._subscribers))
        dependencies = (*sub._broker_dependencies, *sub.calls[0].dependencies)
        assert len(dependencies) == 3, dependencies

    async def test_router_middlewares(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        pub_broker = self.get_broker()

        router = type(router)(middlewares=(1,))
        router2 = type(router)(middlewares=(2,))

        args, kwargs = self.get_subscriber_params(queue, middlewares=(3,))

        @router2.subscriber(*args, **kwargs)
        @router2.publisher(queue, middlewares=(3,))
        def subscriber() -> None: ...

        router.include_router(router2)
        pub_broker.include_routers(router)

        sub = next(iter(pub_broker._subscribers))
        publisher = next(iter(pub_broker._publishers))

        subscriber_middlewares = (
            *sub._broker_middlewares,
            *sub.calls[0].item_middlewares,
        )
        assert subscriber_middlewares == (1, 2, 3)

        publisher_middlewares = (*publisher._broker_middlewares, *publisher.middlewares)
        assert publisher_middlewares == (1, 2, 3)

    async def test_router_include_with_middlewares(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        pub_broker = self.get_broker()

        router2 = type(router)()

        args, kwargs = self.get_subscriber_params(queue, middlewares=(3,))

        @router2.subscriber(*args, **kwargs)
        @router2.publisher(queue, middlewares=(3,))
        def subscriber() -> None: ...

        router.include_router(router2, middlewares=(2,))
        pub_broker.include_router(router, middlewares=(1,))

        sub = next(iter(pub_broker._subscribers))
        publisher = next(iter(pub_broker._publishers))

        sub_middlewares = (*sub._broker_middlewares, *sub.calls[0].item_middlewares)
        assert sub_middlewares == (1, 2, 3), sub_middlewares

        publisher_middlewares = (*publisher._broker_middlewares, *publisher.middlewares)
        assert publisher_middlewares == (1, 2, 3)

    async def test_router_parser(
        self,
        router: BrokerRouter,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        async def parser(msg, original):
            mock.parser()
            return await original(msg)

        async def decoder(msg, original):
            mock.decoder()
            return await original(msg)

        router = type(router)(
            parser=parser,
            decoder=decoder,
        )

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

    async def test_router_parser_override(
        self,
        router: BrokerRouter,
        queue: str,
        mock: Mock,
    ) -> None:
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

        router = type(router)(
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

    async def test_router_in_init(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        args, kwargs = self.get_subscriber_params(queue)

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


@pytest.mark.asyncio()
class RouterLocalTestcase(RouterTestcase):
    async def test_publisher_mock(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

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

    async def test_subscriber_mock(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

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

    async def test_manual_publisher_mock(
        self,
        router: BrokerRouter,
        queue: str,
    ) -> None:
        pub_broker = self.get_broker()

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
