import asyncio
from typing import Any

import pytest

from faststream import BaseMiddleware
from faststream.exceptions import SetupError
from faststream.redis import ListSub, StreamSub
from faststream.redis.testing import FakeProducer
from tests.brokers.base.testclient import BrokerTestclientTestcase
from tests.marks import require_aiopika

from .basic import RedisMemoryTestcaseConfig


@pytest.mark.redis()
@pytest.mark.asyncio()
class TestTestclient(RedisMemoryTestcaseConfig, BrokerTestclientTestcase):
    @pytest.mark.connected()
    async def test_with_real_testclient(self, queue: str, event: asyncio.Event) -> None:
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

    async def test_batch_assert_called_once_with(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(list=ListSub(queue, batch=True))
        async def m(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish_batch({"n": 1}, {"n": 2}, list=queue)

            await m.assert_called_once_with([{"n": 1}, {"n": 2}])

            # A batch has one header set per message, so there is no single answer
            with pytest.raises(SetupError, match="received a batch"):
                await m.assert_called_once_with(headers={"key": "value"})

            with pytest.raises(SetupError, match=r"received a batch.*list"):
                await m.assert_called_once_with(list=queue)

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

    @pytest.mark.parametrize(
        "returned",
        (pytest.param(None, id="None"), pytest.param([], id="Empty Sequence")),
    )
    async def test_batch_publisher_empty_result_matches_default_publisher(
        self,
        queue: str,
        returned: Any,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3056.

        An empty result publishes one empty message, exactly as a non-batch
        publisher does.
        """
        broker = self.get_broker()

        batch_publisher = broker.publisher(list=ListSub(queue + "1", batch=True))
        default_publisher = broker.publisher(list=queue + "2")

        @batch_publisher
        @broker.subscriber(queue)
        async def batched(msg):
            return returned

        @default_publisher
        @broker.subscriber(queue + "3")
        async def single(msg) -> None:
            return None

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            await br.publish("hello", queue + "3")

            default_publisher.mock.assert_called_once_with(b"")
            # Redis batch subscribers decode every element to `str`, so the same
            # empty message on the wire is observed as `""` rather than `b""`.
            batch_publisher.mock.assert_called_once_with([""])

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

    async def test_stream_same_group_delivers_to_one_consumer(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(
            stream=StreamSub(queue, group="workers", consumer="consumer-1"),
        )
        async def subscriber1(msg) -> None: ...

        @broker.subscriber(
            stream=StreamSub(queue, group="workers", consumer="consumer-2"),
        )
        async def subscriber2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", stream=queue)

            # exactly one consumer of the group handles the message
            called = [m for m in (subscriber1.mock, subscriber2.mock) if m.call_count]
            assert len(called) == 1
            called[0].assert_called_once_with("hello")

    async def test_stream_different_groups_all_receive(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(
            stream=StreamSub(queue, group="workers-a", consumer="consumer-1"),
        )
        async def subscriber1(msg) -> None: ...

        @broker.subscriber(
            stream=StreamSub(queue, group="workers-b", consumer="consumer-1"),
        )
        async def subscriber2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", stream=queue)

            subscriber1.mock.assert_called_once_with("hello")
            subscriber2.mock.assert_called_once_with("hello")

    async def test_stream_without_group_always_receives(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(
            stream=StreamSub(queue, group="workers", consumer="consumer-1"),
        )
        async def grouped(msg) -> None: ...

        @broker.subscriber(stream=queue)
        async def ungrouped(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", stream=queue)

            grouped.mock.assert_called_once_with("hello")
            ungrouped.mock.assert_called_once_with("hello")

    async def test_stream_batch_assert_called_once_with(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(stream=StreamSub(queue, batch=True))
        async def m(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", stream=queue)

            await m.assert_called_once_with(["hello"])

            with pytest.raises(SetupError, match=r"received a batch.*stream"):
                await m.assert_called_once_with(stream=queue)

    @pytest.mark.parametrize("kind", ("channel", "list", "stream"))
    async def test_assertions_take_the_redis_fields(self, queue: str, kind: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(**{kind: queue})
        async def handle(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", **{kind: queue})

            await handle.assert_called_once_with("hello", **{kind: queue})
            await handle.assert_called_with(**{kind: queue})
            await handle.assert_any_call(**{kind: queue})

            with pytest.raises(
                AssertionError,
                match=f"{kind}: expected 'other', got '{queue}'",
            ):
                await handle.assert_called_once_with("hello", **{kind: "other"})

    async def test_pattern_assertions_take_the_delivered_channel(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(f"{queue}.{{name}}")
        async def handle(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", f"{queue}.john")

            await handle.assert_called_once_with("hello", channel=f"{queue}.john")

    @pytest.mark.parametrize("kind", ("channel", "list", "stream"))
    async def test_publisher_assertions_take_the_redis_fields(
        self,
        queue: str,
        kind: str,
    ) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(**{kind: queue + "2"})

        @broker.subscriber(queue)
        async def handle(msg) -> None:
            await publisher.publish("response")

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)

            await publisher.assert_called_once_with("response", **{kind: queue + "2"})
            await publisher.assert_called_with(**{kind: queue + "2"})
            await publisher.assert_any_call(**{kind: queue + "2"})

    async def test_redis_fields_refuse_another_kind(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def from_channel(msg) -> None: ...

        @broker.subscriber(stream=queue)
        async def from_stream(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", channel=queue)
            await br.publish("hello", stream=queue)

            with pytest.raises(SetupError, match="`list` was asked of a pub/sub message"):
                await from_channel.assert_called_once_with("hello", list=queue)

            with pytest.raises(
                SetupError, match="`channel` was asked of a stream message"
            ):
                await from_stream.assert_called_once_with("hello", channel=queue)

    @require_aiopika
    async def test_redis_fields_refuse_another_brokers_message(self, queue: str) -> None:
        from faststream.rabbit import RabbitBroker, TestRabbitBroker

        broker = self.get_broker()
        rabbit = RabbitBroker()

        # The first decorator decides the wrapper class: Redis's here
        @rabbit.subscriber(queue)
        @broker.subscriber(queue)
        async def handle(msg) -> None: ...

        async with self.patch_broker(broker), TestRabbitBroker(rabbit):
            await rabbit.publish("hello", queue)

            await handle.assert_called_once_with("hello")

            with pytest.raises(SetupError, match="`channel` is a Redis field"):
                await handle.assert_called_once_with("hello", channel=queue)

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
