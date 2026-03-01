import asyncio

import pytest

from faststream import BaseMiddleware
from faststream.redis import ListSub, StreamSub
from faststream.redis.testing import FakeProducer
from tests.brokers.base.testclient import BrokerTestclientTestcase

from .basic import RedisMemoryTestcaseConfig


@pytest.mark.redis()
@pytest.mark.asyncio()
class TestTestclient(RedisMemoryTestcaseConfig, BrokerTestclientTestcase):
    @pytest.mark.connected()
    async def test_with_real_testclient(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        broker = self.get_broker()

        @broker.subscriber(queue)
        def subscriber(m) -> None:
            event.set()

        async with self.patch_broker(broker, with_real=True) as br:
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()

    async def test_respect_middleware(self, queue: str) -> None:
        routes = []

        class Middleware(BaseMiddleware):
            async def on_receive(self) -> None:
                routes.append(None)
                return await super().on_receive()

        broker = self.get_broker(middlewares=(Middleware,))

        @broker.subscriber(queue)
        async def h1(m) -> None: ...

        @broker.subscriber(queue + "1")
        async def h2(m) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("", queue)
            await br.publish("", queue + "1")

        assert len(routes) == 2

    @pytest.mark.connected()
    async def test_real_respect_middleware(self, queue: str) -> None:
        routes = []

        class Middleware(BaseMiddleware):
            async def on_receive(self) -> None:
                routes.append(None)
                return await super().on_receive()

        broker = self.get_broker(middlewares=(Middleware,))

        @broker.subscriber(queue)
        async def h1(m) -> None: ...

        @broker.subscriber(queue + "1")
        async def h2(m) -> None: ...

        async with self.patch_broker(broker, with_real=True) as br:
            await br.publish("", queue)
            await br.publish("", queue + "1")
            await h1.wait_call(3)
            await h2.wait_call(3)

        assert len(routes) == 2

    async def test_pub_sub_pattern(self) -> None:
        broker = self.get_broker()

        @broker.subscriber("test.{name}")
        async def handler(msg):
            return msg

        async with self.patch_broker(broker) as br:
            assert await (await br.request(1, "test.name.useless")).decode() == 1
            handler.mock.assert_called_once_with(1)

    async def test_list(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(list=queue)
        async def handler(msg):
            return msg

        async with self.patch_broker(broker) as br:
            assert await (await br.request(1, list=queue)).decode() == 1
            handler.mock.assert_called_once_with(1)

    async def test_batch_pub_by_default_pub(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(list=ListSub(queue, batch=True))
        async def m(msg) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", list=queue)
            m.mock.assert_called_once_with(["hello"])

    async def test_batch_pub_by_pub_batch(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(list=ListSub(queue, batch=True))
        async def m(msg) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish_batch("hello", list=queue)
            m.mock.assert_called_once_with(["hello"])

    async def test_batch_publisher_mock(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        batch_list = ListSub(queue + "1", batch=True)
        publisher = broker.publisher(list=batch_list)

        @publisher
        @broker.subscriber(queue)
        async def m(msg):
            return 1, 2, 3

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            m.mock.assert_called_once_with("hello")
            publisher.mock.assert_called_once_with([1, 2, 3])

    async def test_stream(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(stream=queue)
        async def handler(msg):
            return msg

        async with self.patch_broker(broker) as br:
            assert await (await br.request(1, stream=queue)).decode() == 1
            handler.mock.assert_called_once_with(1)

    async def test_stream_batch_pub_by_default_pub(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(stream=StreamSub(queue, batch=True))
        async def m(msg) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", stream=queue)
            m.mock.assert_called_once_with(["hello"])

    async def test_stream_publisher(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        batch_stream = StreamSub(queue + "1")
        publisher = broker.publisher(stream=batch_stream)

        @publisher
        @broker.subscriber(queue)
        async def m(msg):
            return 1, 2, 3

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            m.mock.assert_called_once_with("hello")
            publisher.mock.assert_called_once_with([1, 2, 3])

    async def test_publish_to_none(self) -> None:
        broker = self.get_broker()

        async with self.patch_broker(broker) as br:
            with pytest.raises(ValueError):  # noqa: PT011
                await br.publish("hello")

    @pytest.mark.connected()
    async def test_broker_gets_patched_attrs_within_cm(self) -> None:
        await super().test_broker_gets_patched_attrs_within_cm(FakeProducer)

    @pytest.mark.connected()
    async def test_broker_with_real_doesnt_get_patched(self) -> None:
        await super().test_broker_with_real_doesnt_get_patched()

    @pytest.mark.connected()
    async def test_broker_with_real_patches_publishers_and_subscribers(
        self,
        queue: str,
    ) -> None:
        await super().test_broker_with_real_patches_publishers_and_subscribers(queue)

    @pytest.mark.xfail(reason="https://github.com/ag2ai/faststream/issues/2513")
    async def test_publisher_without_destination(self) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2513."""
        broker = self.get_broker()

        # use two publishers to check that we don't have conflicts
        channel_publisher = broker.publisher(channel="")
        another_channel_publisher = broker.publisher(channel="")

        list_publisher = broker.publisher(list="")
        stream_publisher = broker.publisher(stream="")

        async with self.patch_broker(broker):
            await channel_publisher.publish(None, channel="new-key")
            channel_publisher.mock.assert_called_once()

            await another_channel_publisher.publish(None, channel="new-key")
            another_channel_publisher.mock.assert_called_once()

            await list_publisher.publish(None, list="new-key")
            list_publisher.mock.assert_called_once()

            await stream_publisher.publish(None, stream="new-key")
            stream_publisher.mock.assert_called_once()
