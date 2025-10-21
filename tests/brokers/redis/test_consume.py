import asyncio
from contextlib import suppress
from unittest.mock import MagicMock, call, patch

import pytest
from redis.asyncio import Redis

from faststream import AckPolicy
from faststream.redis import (
    ListSub,
    PubSub,
    RedisMessage,
    RedisStreamMessage,
    StreamSub,
)
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator

from .basic import RedisTestcaseConfig


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
class TestConsume(RedisTestcaseConfig, BrokerRealConsumeTestcase):
    async def test_consume_native(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(queue)
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            result = await br._connection.publish(queue, "hello")
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=3,
            )
            assert result == 1, result

        mock.assert_called_once_with(b"hello")

    async def test_pattern_with_path(
        self,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber("test.{name}")
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", "test.name")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with("hello")

    async def test_pattern_without_path(
        self,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(PubSub("test.*", pattern=True))
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", "test.name")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with("hello")

    @pytest.mark.flaky(reruns=3, reruns_delay=1)
    async def test_concurrent_consume_channel(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        event2 = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(channel=PubSub(queue), max_workers=2)
        async def handler(msg):
            mock()
            if event.is_set():
                event2.set()
            else:
                event.set()
            await asyncio.sleep(0.1)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            for i in range(5):
                await br.publish(i, queue)

        await asyncio.wait(
            (
                asyncio.create_task(event.wait()),
                asyncio.create_task(event2.wait()),
            ),
            timeout=3,
        )

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2, mock.call_count


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
class TestConsumeList(RedisTestcaseConfig):
    async def test_consume_list(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(list=queue)
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", list=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with("hello")

    async def test_consume_list_native(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(list=queue)
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br._connection.rpush(queue, "hello")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with(b"hello")

    @pytest.mark.slow()
    async def test_consume_list_batch_with_one(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            list=ListSub(queue, batch=True, polling_interval=0.01),
        )
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hi", list=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()
            mock.assert_called_once_with(["hi"])

    @pytest.mark.slow()
    async def test_consume_list_batch_headers(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            list=ListSub(queue, batch=True, polling_interval=0.01),
        )
        def subscriber(m, msg: RedisMessage) -> None:
            check = all(
                (
                    msg.headers,
                    msg.headers["correlation_id"]
                    == msg.batch_headers[0]["correlation_id"],
                    msg.headers.get("custom") == "1",
                ),
            )
            mock(check)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish("", list=queue, headers={"custom": "1"}),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()
            mock.assert_called_once_with(True)

    @pytest.mark.slow()
    async def test_consume_list_batch(
        self,
        queue: str,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        msgs_queue = asyncio.Queue(maxsize=1)

        @consume_broker.subscriber(
            list=ListSub(queue, batch=True, polling_interval=0.01),
        )
        async def handler(msg) -> None:
            await msgs_queue.put(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br.publish_batch(1, "hi", list=queue)

            result, _ = await asyncio.wait(
                (asyncio.create_task(msgs_queue.get()),),
                timeout=3,
            )

            assert [{1, "hi"}] == [set(r.result()) for r in result]

    @pytest.mark.slow()
    async def test_consume_list_batch_complex(
        self,
        queue: str,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        from pydantic import BaseModel

        class Data(BaseModel):
            m: str

            def __hash__(self):
                return hash(self.m)

        msgs_queue = asyncio.Queue(maxsize=1)

        @consume_broker.subscriber(
            list=ListSub(queue, batch=True, polling_interval=0.01),
        )
        async def handler(msg: list[Data]) -> None:
            await msgs_queue.put(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br.publish_batch(Data(m="hi"), Data(m="again"), list=queue)

            result, _ = await asyncio.wait(
                (asyncio.create_task(msgs_queue.get()),),
                timeout=3,
            )

        assert [{Data(m="hi"), Data(m="again")}] == [set(r.result()) for r in result]

    @pytest.mark.slow()
    async def test_consume_list_batch_native(
        self,
        queue: str,
    ) -> None:
        consume_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=1)

        @consume_broker.subscriber(
            list=ListSub(queue, batch=True, polling_interval=0.01),
        )
        async def handler(msg) -> None:
            await msgs_queue.put(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br._connection.rpush(queue, 1, "hi")

            result, _ = await asyncio.wait(
                (asyncio.create_task(msgs_queue.get()),),
                timeout=3,
            )

        assert [{1, "hi"}] == [set(r.result()) for r in result]

    async def test_get_one(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(list=queue)

        async with self.patch_broker(broker) as br:
            await br.start()

            message = None

            async def consume() -> None:
                nonlocal message
                message = await subscriber.get_one(timeout=5)

            async def publish() -> None:
                await br.publish("test_message", list=queue)

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish()),
                ),
                timeout=10,
            )

            assert message is not None
            assert await message.decode() == "test_message"

    async def test_get_one_timeout(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(list=queue)

        async with self.patch_broker(broker) as br:
            await br.start()

            mock(await subscriber.get_one(timeout=1e-24))
            mock.assert_called_once_with(None)

    async def test_concurrent_consume_list(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        event2 = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(list=ListSub(queue), max_workers=2)
        async def handler(msg):
            mock()
            if event.is_set():
                event2.set()
            else:
                event.set()
            await asyncio.sleep(0.1)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            for i in range(5):
                await br.publish(i, list=queue)

            await asyncio.wait(
                (
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2, mock.call_count

    async def test_iterator(
        self,
        queue: str,
    ) -> None:
        expected_messages = ("test_message_1", "test_message_2")

        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(list=queue)

        async with self.patch_broker(broker) as br:
            await br.start()

            async def publish_test_message():
                for msg in expected_messages:
                    await br.publish(msg, list=queue)

            _ = await asyncio.create_task(publish_test_message())

            index_message = 0
            async for msg in subscriber:
                result_message = await msg.decode()

                assert result_message == expected_messages[index_message]

                index_message += 1
                if index_message >= len(expected_messages):
                    break


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
class TestConsumeStream(RedisTestcaseConfig):
    @pytest.mark.slow()
    async def test_consume_stream(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(stream=StreamSub(queue, polling_interval=10))
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", stream=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with("hello")

    @pytest.mark.slow()
    async def test_consume_stream_with_big_interval(
        self,
        event: asyncio.Event,
        mock: MagicMock,
        queue: str,
    ) -> None:
        consume_broker = self.get_broker()

        @consume_broker.subscriber(stream=StreamSub(queue, polling_interval=100000))
        async def handler(msg):
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", stream=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with("hello")

    @pytest.mark.slow()
    async def test_consume_stream_native(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(stream=StreamSub(queue, polling_interval=10))
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br._connection.xadd(queue, {"message": "hello"}),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with({"message": "hello"})

    @pytest.mark.slow()
    @pytest.mark.flaky(reruns=3, reruns_delay=1)
    async def test_consume_stream_batch(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(queue, polling_interval=10, batch=True),
        )
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", stream=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with(["hello"])

    @pytest.mark.slow()
    async def test_consume_stream_batch_headers(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(queue, polling_interval=10, batch=True),
        )
        def subscriber(m, msg: RedisMessage) -> None:
            check = all(
                (
                    msg.headers,
                    msg.headers["correlation_id"]
                    == msg.batch_headers[0]["correlation_id"],
                    msg.headers.get("custom") == "1",
                ),
            )
            mock(check)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish("", stream=queue, headers={"custom": "1"}),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()
            mock.assert_called_once_with(True)

    @pytest.mark.slow()
    async def test_consume_stream_batch_complex(
        self,
        queue: str,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        from pydantic import BaseModel

        class Data(BaseModel):
            m: str

        msgs_queue = asyncio.Queue(maxsize=1)

        @consume_broker.subscriber(
            stream=StreamSub(queue, polling_interval=10, batch=True),
        )
        async def handler(msg: list[Data]) -> None:
            await msgs_queue.put(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br.publish(Data(m="hi"), stream=queue)

            result, _ = await asyncio.wait(
                (asyncio.create_task(msgs_queue.get()),),
                timeout=3,
            )

        assert next(iter(result)).result() == [Data(m="hi")]

    @pytest.mark.slow()
    async def test_consume_stream_batch_native(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(queue, polling_interval=10, batch=True),
        )
        async def handler(msg) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br._connection.xadd(queue, {"message": "hello"}),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with([{"message": "hello"}])

    async def test_consume_group(
        self,
        queue: str,
    ) -> None:
        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue),
        )
        async def handler(msg: RedisMessage) -> None: ...

        assert next(iter(consume_broker.subscribers)).last_id == ">"

    async def test_consume_group_with_last_id(
        self,
        queue: str,
    ) -> None:
        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue, last_id="0"),
        )
        async def handler(msg: RedisMessage) -> None: ...

        assert next(iter(consume_broker.subscribers)).last_id == "0"

    async def test_consume_nack(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue),
        )
        async def handler(msg: RedisMessage) -> None:
            event.set()
            await msg.nack()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(Redis, "xack", spy_decorator(Redis.xack)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", stream=queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )

                assert not m.mock.called

        assert event.is_set()

    async def test_consume_ack(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue),
        )
        async def handler(msg: RedisMessage) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(Redis, "xack", spy_decorator(Redis.xack)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", stream=queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )

                m.mock.assert_called_once()

        assert event.is_set()

    @pytest.mark.flaky(reruns=3, reruns_delay=1)
    async def test_consume_and_delete_acked(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue),
        )
        async def handler(msg: RedisStreamMessage) -> None:
            event.set()
            await msg.delete(consume_broker._connection)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(Redis, "xdel", spy_decorator(Redis.xdel)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", stream=queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )

                m.mock.assert_called_once()

            queue_len = await br._connection.xlen(queue)
            assert queue_len == 0, (
                f"Redis stream must be empty here, found {queue_len} messages"
            )

    async def test_consume_and_delete_nacked(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue),
            ack_policy=AckPolicy.MANUAL,
        )
        async def handler(msg: RedisStreamMessage) -> None:
            assert not msg.committed
            await msg.delete(consume_broker._connection)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(Redis, "xdel", spy_decorator(Redis.xdel)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", stream=queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )

                m.mock.assert_called_once()

            queue_len = await br._connection.xlen(queue)
            assert queue_len == 0, (
                f"Redis stream must be empty here, found {queue_len} messages"
            )

    async def test_get_one(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(stream=queue)

        async with self.patch_broker(broker) as br:
            await br.start()

            message = None

            async def consume() -> None:
                nonlocal message
                message = await subscriber.get_one(timeout=3)

            async def publish() -> None:
                await asyncio.sleep(0.1)
                await br.publish("test_message", stream=queue)

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish()),
                ),
                timeout=10,
            )

            assert message is not None
            assert await message.decode() == "test_message"

    async def test_get_one_timeout(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(stream=queue)

        async with self.patch_broker(broker) as br:
            await br.start()

            mock(await subscriber.get_one(timeout=1e-24))
            mock.assert_called_once_with(None)

    @pytest.mark.flaky(reruns=3, reruns_delay=1)
    async def test_concurrent_consume_stream(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        event2 = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(stream=StreamSub(queue), max_workers=2)
        async def handler(msg: RedisStreamMessage) -> None:
            mock()
            if event.is_set():
                event2.set()
            else:
                event.set()
            await asyncio.sleep(0.1)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            for i in range(5):
                await br.publish(i, stream=queue)

        await asyncio.wait(
            (
                asyncio.create_task(event.wait()),
                asyncio.create_task(event2.wait()),
            ),
            timeout=3,
        )

        assert mock.call_count == 2, mock.call_count

    async def test_iterator(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        expected_messages = ("test_message_1", "test_message_2")

        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(stream=queue)

        async with self.patch_broker(broker) as br:
            await br.start()

            async def publish_test_message() -> None:
                await asyncio.sleep(0.1)
                for msg in expected_messages:
                    await br.publish(msg, stream=queue)

            async def consume() -> None:
                index_message = 0
                async for msg in subscriber:
                    result_message = await msg.decode()

                    mock(result_message)

                    index_message += 1
                    if index_message >= len(expected_messages):
                        break

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish_test_message()),
                ),
                timeout=self.timeout,
            )

            calls = [call(msg) for msg in expected_messages]
            mock.assert_has_calls(calls=calls)

    @pytest.mark.slow()
    async def test_consume_stream_with_min_idle_time(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Test consuming messages using XAUTOCLAIM with min_idle_time."""
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="test_group",
                consumer="consumer1",
                min_idle_time=100,  # 100ms
            ),
        )
        async def handler(msg: str) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # First, publish a message and let it become pending
            await br.publish("pending_message", stream=queue)

            # Wait a bit to ensure message becomes idle
            await asyncio.sleep(0.2)

            # The subscriber with XAUTOCLAIM should reclaim it
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=3,
            )

            assert event.is_set()
            mock.assert_called_once_with("pending_message")

    @pytest.mark.slow()
    async def test_get_one_with_min_idle_time(
        self,
        queue: str,
    ) -> None:
        """Test get_one() with min_idle_time."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await br.start()

            # First, create a pending message
            await br._connection.xadd(queue, {"data": "pending"})
            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "idle_group", id="0", mkstream=True
                )

            # Read it but don't ack to make it pending
            await br._connection.xreadgroup(
                groupname="idle_group",
                consumername="temp_consumer",
                streams={queue: ">"},
                count=1,
            )

            # Wait for it to become idle
            await asyncio.sleep(0.1)

            # Now use get_one with min_idle_time
            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="idle_group",
                    consumer="claiming_consumer",
                    min_idle_time=1,
                )
            )

            message = await subscriber.get_one(timeout=3)

            assert message is not None
            decoded = await message.decode()
            assert decoded == b"pending"

    @pytest.mark.slow()
    async def test_get_one_with_min_idle_time_no_pending(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Test get_one() with min_idle_time when no pending messages."""
        broker = self.get_broker(apply_types=True)

        subscriber = broker.subscriber(
            stream=StreamSub(
                queue,
                group="empty_group",
                consumer="consumer1",
                min_idle_time=100,
            )
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            # Should return None after timeout
            result = await subscriber.get_one(timeout=0.5)
            mock(result)

            mock.assert_called_once_with(None)

    @pytest.mark.slow()
    async def test_iterator_with_min_idle_time(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Test async iterator with min_idle_time."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await br.start()

            # Create pending messages
            await br._connection.xadd(queue, {"data": "msg1"})
            await br._connection.xadd(queue, {"data": "msg2"})

            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "iter_group", id="0", mkstream=True
                )

            # Read them but don't ack
            await br._connection.xreadgroup(
                groupname="iter_group",
                consumername="temp",
                streams={queue: ">"},
                count=10,
            )

            await asyncio.sleep(0.1)

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="iter_group",
                    consumer="iter_consumer",
                    min_idle_time=1,
                )
            )

            count = 0
            async for msg in subscriber:
                decoded = await msg.decode()
                mock(decoded)
                count += 1
                if count >= 2:
                    break

            assert count == 2
            mock.assert_any_call(b"msg1")
            mock.assert_any_call(b"msg2")

    @pytest.mark.slow()
    async def test_consume_stream_batch_with_min_idle_time(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Test batch consuming with min_idle_time."""
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="batch_group",
                consumer="batch_consumer",
                batch=True,
                min_idle_time=1,
            ),
        )
        async def handler(msg: list) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # Create a pending message first
            await br._connection.xadd(queue, {"data": "batch_msg"})

            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "batch_group", id="0", mkstream=True
                )

            # Read but don't ack
            await br._connection.xreadgroup(
                groupname="batch_group",
                consumername="temp",
                streams={queue: ">"},
                count=1,
            )

            await asyncio.sleep(0.1)

            # Now the subscriber should reclaim it
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=3,
            )

            assert event.is_set()
            # In batch mode, should receive list
            assert mock.call_count == 1
            called_with = mock.call_args[0][0]
            assert isinstance(called_with, list)
            assert len(called_with) > 0

    @pytest.mark.slow()
    async def test_xautoclaim_with_deleted_messages(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Test XAUTOCLAIM behavior when messages are deleted from stream."""
        consume_broker = self.get_broker(apply_types=True)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # Create and consume a message without ack
            msg_id = await br._connection.xadd(queue, {"data": "will_delete"})

            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "delete_group", id="0", mkstream=True
                )

            # Read to make it pending
            await br._connection.xreadgroup(
                groupname="delete_group",
                consumername="temp",
                streams={queue: ">"},
                count=1,
            )

            # Delete the message from stream
            await br._connection.xdel(queue, msg_id)

            await asyncio.sleep(0.1)

            # XAUTOCLAIM should handle deleted messages gracefully
            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="delete_group",
                    consumer="delete_consumer",
                    min_idle_time=1,
                )
            )

            # Should timeout gracefully without errors
            result = await subscriber.get_one(timeout=0.5)
            mock(result)

            # Should return None (no valid messages to claim)
            mock.assert_called_once_with(None)

    @pytest.mark.slow()
    async def test_xautoclaim_circular_scanning_with_idle_timeout(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Test that XAUTOCLAIM scans circularly and claims messages as they become idle."""
        consume_broker = self.get_broker(apply_types=True)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # Create multiple pending messages
            msg_ids = []
            for i in range(5):
                msg_id = await br._connection.xadd(queue, {"data": f"msg{i}"})
                msg_ids.append(msg_id)

            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "circular_group", id="0", mkstream=True
                )

            # Read all messages with consumer1 but don't ack - making them pending
            await br._connection.xreadgroup(
                groupname="circular_group",
                consumername="consumer1",
                streams={queue: ">"},
                count=10,
            )

            # Wait for messages to become idle
            await asyncio.sleep(0.1)

            # Create subscriber with XAUTOCLAIM
            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="circular_group",
                    consumer="consumer2",
                    min_idle_time=1,
                )
            )

            # First pass: claim all messages one by one
            claimed_messages_first_pass = []
            for _ in range(5):
                msg = await subscriber.get_one(timeout=1)
                if msg:
                    decoded = await msg.decode()
                    claimed_messages_first_pass.append(decoded)
                    mock(f"first_pass_{decoded.decode()}")

            # Should have claimed all 5 messages in order
            assert len(claimed_messages_first_pass) == 5
            assert claimed_messages_first_pass == [
                b"msg0",
                b"msg1",
                b"msg2",
                b"msg3",
                b"msg4",
            ]

            # After reaching the end, XAUTOCLAIM should restart from "0-0"
            # and scan circularly - messages are still pending since we didn't ACK them
            # Second pass: verify circular behavior by claiming messages again
            msg = await subscriber.get_one(timeout=1)
            assert msg is not None
            decoded = await msg.decode()
            # Should get msg0 again (circular scan restarted)
            assert decoded == b"msg0"
            mock("second_pass_msg0")

            # Verify messages were claimed in both passes
            mock.assert_any_call("first_pass_msg0")
            mock.assert_any_call("second_pass_msg0")
