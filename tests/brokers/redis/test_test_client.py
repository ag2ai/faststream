import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Literal

import pytest

from faststream import BaseMiddleware
from faststream.exceptions import SetupError
from faststream.redis import (
    ListSub,
    PubSub,
    RedisBroker,
    RedisPublishCommand,
    StreamSub,
)
from faststream.redis.testing import FakeProducer
from tests.brokers.base.testclient import BrokerTestclientTestcase

from .basic import RedisMemoryTestcaseConfig


@pytest.mark.redis()
@pytest.mark.asyncio()
class TestTestclient(RedisMemoryTestcaseConfig, BrokerTestclientTestcase):
    @pytest.mark.connected()
    async def test_with_real_testclient(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue)
        def subscriber(m: Any) -> None:
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
        routes: list[Any] = []

        class Middleware(BaseMiddleware):
            async def on_receive(self) -> None:
                routes.append(None)
                return await super().on_receive()

        broker = self.get_broker(middlewares=(Middleware,))

        @broker.subscriber(queue)
        async def h1(m: Any) -> None: ...

        @broker.subscriber(queue + "1")
        async def h2(m: Any) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("", queue)
            await br.publish("", queue + "1")

        assert len(routes) == 2

    @pytest.mark.connected()
    async def test_real_respect_middleware(self, queue: str) -> None:
        routes: list[Any] = []

        class Middleware(BaseMiddleware):
            async def on_receive(self) -> None:
                routes.append(None)
                return await super().on_receive()

        broker = self.get_broker(middlewares=(Middleware,))

        @broker.subscriber(queue)
        async def h1(m: Any) -> None: ...

        @broker.subscriber(queue + "1")
        async def h2(m: Any) -> None: ...

        async with self.patch_broker(broker, with_real=True) as br:
            await br.publish("", queue)
            await br.publish("", queue + "1")
            await h1.wait_call(3)
            await h2.wait_call(3)

        assert len(routes) == 2

    async def test_publish_to_channel_subscription(self, queue: str) -> None:
        broker: RedisBroker = self.get_broker()
        channel = PubSub(queue, pattern=True, polling_interval=0.5)

        @broker.subscriber(channel=channel)
        async def handler(msg: str) -> None: ...

        async with self.patch_broker(broker):
            await broker.publish("hello", channel=channel)

            handler.mock.assert_called_once_with("hello")

        assert (
            channel.name,
            channel.address.template,
            channel.pattern,
            channel.polling_interval,
        ) == (queue, queue, True, 0.5)

    async def test_pub_sub_pattern(self) -> None:
        broker = self.get_broker()

        @broker.subscriber("test.{name}")
        async def handler(msg: Any) -> Any:
            return msg

        async with self.patch_broker(broker) as br:
            assert await (await br.request(1, "test.name.useless")).decode() == 1
            handler.mock.assert_called_once_with(1)

    @pytest.mark.parametrize(
        "use_object",
        (False, True),
        ids=("string", "subscription"),
    )
    async def test_batch_pub_by_default_pub(
        self,
        queue: str,
        use_object: bool,
    ) -> None:
        broker: RedisBroker = self.get_broker()
        list_sub = ListSub(queue, batch=True)
        destination = list_sub if use_object else queue

        @broker.subscriber(list=list_sub)
        async def handler(msg: list[str]) -> None: ...

        async with self.patch_broker(broker):
            await broker.publish("hello", list=destination)

            handler.mock.assert_called_once_with(["hello"])

    @pytest.mark.parametrize(
        "use_object",
        (False, True),
        ids=("string", "subscription"),
    )
    async def test_batch_pub_by_pub_batch(
        self,
        queue: str,
        use_object: bool,
    ) -> None:
        broker: RedisBroker = self.get_broker()
        list_sub = ListSub(
            queue,
            batch=True,
            max_records=5,
            polling_interval=0.2,
        )
        destination = list_sub if use_object else queue

        @broker.subscriber(list=list_sub)
        async def handler(msg: list[str]) -> None: ...

        async with self.patch_broker(broker):
            await broker.publish_batch("hello", "again", list=destination)

            handler.mock.assert_called_once_with(["hello", "again"])

        assert (
            list_sub.name,
            list_sub.batch,
            list_sub.max_records,
            list_sub.polling_interval,
        ) == (queue, True, 5, 0.2)

    async def test_batch_assert_called_once_with(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(list=ListSub(queue, batch=True))
        async def m(msg: Any) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish_batch({"n": 1}, {"n": 2}, list=queue)

            await m.assert_called_once_with([{"n": 1}, {"n": 2}])

            # A batch has one header set per message, so there is no single answer
            with pytest.raises(SetupError, match="received a batch"):
                await m.assert_called_once_with(headers={"key": "value"})

    async def test_publish_batch_to_single_list(self, queue: str) -> None:
        broker = self.get_broker()
        received: list[Any] = []

        @broker.subscriber(list=queue)
        async def handler(msg: Any) -> None:
            received.append(msg)

        async with self.patch_broker(broker) as br:
            await br.publish_batch(1, "hi", {"n": 2}, list=queue)

        assert received == [1, "hi", {"n": 2}]

    async def test_batch_publisher_mock(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        batch_list = ListSub(queue + "1", batch=True)
        publisher = broker.publisher(list=batch_list)

        @publisher
        @broker.subscriber(queue)
        async def m(msg: Any) -> Any:
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
        async def batched(msg: Any) -> Any:
            return returned

        @default_publisher
        @broker.subscriber(queue + "3")
        async def single(msg: Any) -> None:
            return None

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            await br.publish("hello", queue + "3")

            default_publisher.mock.assert_called_once_with(b"")
            # Redis batch subscribers decode every element to `str`, so the same
            # empty message on the wire is observed as `""` rather than `b""`.
            batch_publisher.mock.assert_called_once_with([""])

    @pytest.mark.parametrize(
        "use_object",
        (False, True),
        ids=("string", "subscription"),
    )
    async def test_stream_batch_pub_by_default_pub(
        self,
        queue: str,
        use_object: bool,
    ) -> None:
        broker: RedisBroker = self.get_broker()
        stream = StreamSub(
            queue,
            group="test-group",
            consumer="consumer",
            batch=True,
        )
        destination = stream if use_object else queue

        @broker.subscriber(stream=stream)
        async def handler(msg: list[str]) -> None: ...

        async with self.patch_broker(broker):
            await broker.publish("hello", stream=destination)

            handler.mock.assert_called_once_with(["hello"])

    @pytest.mark.parametrize("method", ("publish", "request"))
    @pytest.mark.parametrize(
        ("maxlen", "expected_maxlen"),
        (
            pytest.param(None, 100, id="subscription-default"),
            pytest.param(10, 10, id="explicit-value"),
            pytest.param(0, 0, id="explicit-zero"),
        ),
    )
    async def test_stream_subscription_maxlen(
        self,
        queue: str,
        method: Literal["publish", "request"],
        maxlen: int | None,
        expected_maxlen: int,
    ) -> None:
        observed: list[tuple[str, int | None]] = []

        class CaptureMiddleware(BaseMiddleware[RedisPublishCommand]):
            async def publish_scope(
                self,
                call_next: Callable[[RedisPublishCommand], Awaitable[Any]],
                cmd: RedisPublishCommand,
            ) -> Any:
                observed.append((cmd.destination, cmd.maxlen))
                return await call_next(cmd)

        broker: RedisBroker = self.get_broker(
            middlewares=(CaptureMiddleware,),
        )
        stream = StreamSub(
            queue,
            group="workers",
            consumer="consumer",
            maxlen=100,
        )

        @broker.subscriber(stream=stream)
        async def handler(msg: str) -> str:
            return msg

        async with self.patch_broker(broker):
            if method == "publish":
                await broker.publish("hello", stream=stream, maxlen=maxlen)
            else:
                _ = await broker.request(
                    "hello",
                    stream=stream,
                    maxlen=maxlen,
                    timeout=self.timeout,
                )

        assert observed == [(queue, expected_maxlen)]
        assert (
            stream.name,
            stream.group,
            stream.consumer,
            stream.maxlen,
            stream.last_id,
        ) == (queue, "workers", "consumer", 100, ">")

    async def test_stream_publisher(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        batch_stream = StreamSub(queue + "1")
        publisher = broker.publisher(stream=batch_stream)

        @publisher
        @broker.subscriber(queue)
        async def m(msg: Any) -> Any:
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
        async def subscriber1(msg: Any) -> None: ...

        @broker.subscriber(
            stream=StreamSub(queue, group="workers", consumer="consumer-2"),
        )
        async def subscriber2(msg: Any) -> None: ...

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
        async def subscriber1(msg: Any) -> None: ...

        @broker.subscriber(
            stream=StreamSub(queue, group="workers-b", consumer="consumer-1"),
        )
        async def subscriber2(msg: Any) -> None: ...

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
        async def grouped(msg: Any) -> None: ...

        @broker.subscriber(stream=queue)
        async def ungrouped(msg: Any) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", stream=queue)

            grouped.mock.assert_called_once_with("hello")
            ungrouped.mock.assert_called_once_with("hello")

    async def test_publish_to_none(self) -> None:
        broker = self.get_broker()

        async with self.patch_broker(broker) as br:
            with pytest.raises(ValueError):  # ruff: ignore[pytest-raises-too-broad]
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
