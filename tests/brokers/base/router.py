import asyncio
from typing import Type
from unittest.mock import Mock

import pytest

from faststream import BaseMiddleware, Depends
from faststream.broker.core.usecase import BrokerUsecase
from faststream.broker.router import ArgsContainer, BrokerRouter, SubscriberRoute
from faststream.types import AnyCallable
from tests.brokers.base.middlewares import LocalMiddlewareTestcase
from tests.brokers.base.parser import LocalCustomParserTestcase


@pytest.mark.asyncio
class RouterTestcase(
    LocalMiddlewareTestcase,
    LocalCustomParserTestcase,
):
    build_message: AnyCallable
    route_class: Type[SubscriberRoute]
    publisher_class: Type[ArgsContainer]

    def patch_broker(self, br: BrokerUsecase, router: BrokerRouter) -> BrokerUsecase:
        br.include_router(router)
        return br

    @pytest.fixture
    def pub_broker(self, broker):
        return broker

    @pytest.fixture
    def raw_broker(self, pub_broker):
        return pub_broker

    async def test_empty_prefix(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
    ):
        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        def subscriber(m):
            event.set()

        pub_broker.include_router(router)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_not_empty_prefix(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
    ):
        router.prefix = "test_"

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        def subscriber(m):
            event.set()

        pub_broker.include_router(router)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_include_with_prefix(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
    ):
        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        def subscriber(m):
            event.set()

        pub_broker.include_router(router, prefix="test_")

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_empty_prefix_publisher(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
    ):
        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        @router.publisher(queue + "resp")
        def subscriber(m):
            return "hi"

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @router.subscriber(*args2, **kwargs2)
        def response(m):
            event.set()

        pub_broker.include_router(router)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_not_empty_prefix_publisher(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
    ):
        router.prefix = "test_"

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        @router.publisher(queue + "resp")
        def subscriber(m):
            return "hi"

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @router.subscriber(*args2, **kwargs2)
        def response(m):
            event.set()

        pub_broker.include_router(router)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_include_publisher_with_prefix(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        @pub_broker.subscriber(f"test_{queue}")
        async def handler(m):
            event.set()

        publisher = router.publisher(response_topic)
        pub_broker.include_router(router, prefix="test_")

        async with pub_broker:
            await pub_broker.start()

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
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
    ):
        router.prefix = "test_"

        p = router.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def subscriber(m):
            await p.publish("resp")

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @router.subscriber(*args2, **kwargs2)
        def response(m):
            event.set()

        pub_broker.include_router(router)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_delayed_handlers(
        self,
        event: asyncio.Event,
        router: BrokerRouter,
        queue: str,
        pub_broker: BrokerUsecase,
    ):
        def response(m):
            event.set()

        args, kwargs = self.get_subscriber_params(queue)

        r = type(router)(
            prefix="test_",
            handlers=(self.route_class(response, *args, **kwargs),),
        )

        pub_broker.include_router(r)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_delayed_publishers(
        self,
        event: asyncio.Event,
        router: BrokerRouter,
        queue: str,
        pub_broker: BrokerUsecase,
        mock: Mock,
    ):
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
        async def handler(msg):
            mock(msg)
            event.set()

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", f"test_{queue}")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

            mock.assert_called_once_with("hello")

    async def test_nested_routers_sub(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
        mock: Mock,
    ):
        core_router = type(router)(prefix="test1_")
        router.prefix = "test2_"

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        def subscriber(m):
            event.set()
            mock(m)
            return "hi"

        core_router.include_routers(router)
        pub_broker.include_routers(core_router)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        pub_broker.publish("hello", f"test1_test2_{queue}")
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            mock.assert_called_with("hello")

    async def test_nested_routers_pub(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
    ):
        core_router = type(router)(prefix="test1_")
        router.prefix = "test2_"

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        @router.publisher(queue + "resp")
        def subscriber(m):
            return "hi"

        args2, kwargs2 = self.get_subscriber_params(
            "test1_" + "test2_" + queue + "resp"
        )

        @pub_broker.subscriber(*args2, **kwargs2)
        def response(m):
            event.set()

        core_router.include_routers(router)
        pub_broker.include_routers(core_router)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        pub_broker.publish("hello", f"test1_test2_{queue}")
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()

    async def test_router_dependencies(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
    ):
        router = type(router)(dependencies=(Depends(lambda: 1),))
        router2 = type(router)(dependencies=(Depends(lambda: 2),))

        args, kwargs = self.get_subscriber_params(
            queue, dependencies=(Depends(lambda: 3),)
        )

        @router2.subscriber(*args, **kwargs)
        def subscriber(): ...

        router.include_router(router2)
        pub_broker.include_routers(router)

        sub = next(iter(pub_broker._subscribers.values()))
        assert len((*sub._broker_dependencies, *sub.calls[0].dependencies)) == 3

    async def test_router_include_with_dependencies(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
    ):
        router2 = type(router)()

        args, kwargs = self.get_subscriber_params(
            queue,
            dependencies=(Depends(lambda: 3),),
        )

        @router2.subscriber(*args, **kwargs)
        def subscriber(): ...

        router.include_router(router2, dependencies=(Depends(lambda: 2),))
        pub_broker.include_router(router, dependencies=(Depends(lambda: 1),))

        sub = next(iter(pub_broker._subscribers.values()))
        dependencies = (*sub._broker_dependencies, *sub.calls[0].dependencies)
        assert len(dependencies) == 3, dependencies

    async def test_router_middlewares(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
    ):
        router = type(router)(middlewares=(BaseMiddleware,))
        router2 = type(router)(middlewares=(BaseMiddleware,))

        args, kwargs = self.get_subscriber_params(queue, middlewares=(3,))

        @router2.subscriber(*args, **kwargs)
        @router2.publisher(queue, middlewares=(3,))
        def subscriber(): ...

        router.include_router(router2)
        pub_broker.include_routers(router)

        sub = next(iter(pub_broker._subscribers.values()))
        publisher = next(iter(pub_broker._publishers.values()))

        assert len((*sub._broker_middlewares, *sub.calls[0].item_middlewares)) == 3
        assert len((*publisher._broker_middlewares, *publisher._middlewares)) == 3

    async def test_router_include_with_middlewares(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
    ):
        router2 = type(router)()

        args, kwargs = self.get_subscriber_params(queue, middlewares=(3,))

        @router2.subscriber(*args, **kwargs)
        @router2.publisher(queue, middlewares=(3,))
        def subscriber(): ...

        router.include_router(router2, middlewares=(BaseMiddleware,))
        pub_broker.include_router(router, middlewares=(BaseMiddleware,))

        sub = next(iter(pub_broker._subscribers.values()))
        publisher = next(iter(pub_broker._publishers.values()))

        sub_middlewares = (*sub._broker_middlewares, *sub.calls[0].item_middlewares)
        assert len(sub_middlewares) == 3, sub_middlewares
        assert len((*publisher._broker_middlewares, *publisher._middlewares)) == 3

    async def test_router_parser(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
        mock: Mock,
    ):
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
        def subscriber(s):
            event.set()

        pub_broker.include_routers(router)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", queue)),
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
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
        mock: Mock,
    ):
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
        def subscriber(s):
            event.set()

        pub_broker.include_routers(router)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            assert not mock.called
            mock.parser.assert_called_once()
            mock.decoder.assert_called_once()


@pytest.mark.asyncio
class RouterLocalTestcase(RouterTestcase):
    @pytest.fixture
    def pub_broker(self, test_broker):
        return test_broker

    async def test_publisher_mock(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
    ):
        pub = router.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        @pub
        def subscriber(m):
            event.set()
            return "hi"

        pub_broker.include_router(router)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            pub.mock.assert_called_with("hi")

    async def test_subscriber_mock(
        self,
        router: BrokerRouter,
        pub_broker: BrokerUsecase,
        queue: str,
        event: asyncio.Event,
    ):
        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        def subscriber(m):
            event.set()
            return "hi"

        pub_broker.include_router(router)

        async with pub_broker:
            await pub_broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(pub_broker.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            subscriber.mock.assert_called_with("hello")

    async def test_manual_publisher_mock(
        self, router: BrokerRouter, queue: str, pub_broker: BrokerUsecase
    ):
        publisher = router.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def m(m):
            await publisher.publish("response")

        pub_broker.include_router(router)
        async with pub_broker:
            await pub_broker.start()
            await pub_broker.publish("hello", queue)
            publisher.mock.assert_called_with("response")
