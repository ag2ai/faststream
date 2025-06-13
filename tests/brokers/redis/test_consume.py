import asyncio
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from redis.asyncio import Redis

from faststream.redis import ListSub, PubSub, RedisBroker, RedisMessage, StreamSub
from faststream.redis.annotations import RedisStreamMessage
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator


@pytest.mark.redis
@pytest.mark.asyncio
class TestConsume(BrokerRealConsumeTestcase):
    def get_broker(self, apply_types: bool = False):
        return RedisBroker(apply_types=apply_types)

    async def test_consume_native(
        self,
        event: asyncio.Event,
        mock: MagicMock,
        queue: str,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber(queue)
        async def handler(msg):
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br._connection.publish(queue, "hello")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with(b"hello")

    async def test_pattern_with_path(
        self,
        event: asyncio.Event,
        mock: MagicMock,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber("test.{name}")
        async def handler(msg):
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
        event: asyncio.Event,
        mock: MagicMock,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber(PubSub("test.*", pattern=True))
        async def handler(msg):
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


@pytest.mark.redis
@pytest.mark.asyncio
class TestConsumeList:
    def get_broker(self, apply_types: bool = False):
        return RedisBroker(apply_types=apply_types)

    def patch_broker(self, broker):
        return broker

    async def test_consume_list(
        self,
        event: asyncio.Event,
        queue: str,
        mock: MagicMock,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber(list=queue)
        async def handler(msg):
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
        event: asyncio.Event,
        queue: str,
        mock: MagicMock,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber(list=queue)
        async def handler(msg):
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

    @pytest.mark.slow
    async def test_consume_list_batch_with_one(
        self,
        queue: str,
        event: asyncio.Event,
        mock,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            list=ListSub(queue, batch=True, polling_interval=0.01)
        )
        async def handler(msg):
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

    @pytest.mark.slow
    async def test_consume_list_batch_headers(
        self,
        queue: str,
        event: asyncio.Event,
        mock,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            list=ListSub(queue, batch=True, polling_interval=0.01)
        )
        def subscriber(m, msg: RedisMessage):
            check = all(
                (
                    msg.headers,
                    msg.headers["correlation_id"]
                    == msg.batch_headers[0]["correlation_id"],
                    msg.headers.get("custom") == "1",
                )
            )
            mock(check)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish("", list=queue, headers={"custom": "1"})
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()
            mock.assert_called_once_with(True)

    @pytest.mark.slow
    async def test_consume_list_batch(
        self,
        queue: str,
    ):
        consume_broker = self.get_broker(apply_types=True)

        msgs_queue = asyncio.Queue(maxsize=1)

        @consume_broker.subscriber(
            list=ListSub(queue, batch=True, polling_interval=0.01)
        )
        async def handler(msg):
            await msgs_queue.put(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br.publish_batch(1, "hi", list=queue)

            result, _ = await asyncio.wait(
                (asyncio.create_task(msgs_queue.get()),),
                timeout=3,
            )

            assert [{1, "hi"}] == [set(r.result()) for r in result]

    @pytest.mark.slow
    async def test_consume_list_batch_complex(
        self,
        queue: str,
    ):
        consume_broker = self.get_broker(apply_types=True)

        from pydantic import BaseModel

        class Data(BaseModel):
            m: str

            def __hash__(self):
                return hash(self.m)

        msgs_queue = asyncio.Queue(maxsize=1)

        @consume_broker.subscriber(
            list=ListSub(queue, batch=True, polling_interval=0.01)
        )
        async def handler(msg: List[Data]):
            await msgs_queue.put(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br.publish_batch(Data(m="hi"), Data(m="again"), list=queue)

            result, _ = await asyncio.wait(
                (asyncio.create_task(msgs_queue.get()),),
                timeout=3,
            )

        assert [{Data(m="hi"), Data(m="again")}] == [set(r.result()) for r in result]

    @pytest.mark.slow
    async def test_consume_list_batch_native(
        self,
        queue: str,
    ):
        consume_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=1)

        @consume_broker.subscriber(
            list=ListSub(queue, batch=True, polling_interval=0.01)
        )
        async def handler(msg):
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
        event: asyncio.Event,
    ):
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(list=queue)

        async with self.patch_broker(broker) as br:
            await br.start()

            message = None

            async def consume():
                nonlocal message
                message = await subscriber.get_one(timeout=5)

            async def publish():
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
    ):
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(list=queue)

        async with self.patch_broker(broker) as br:
            await br.start()

            mock(await subscriber.get_one(timeout=1e-24))
            mock.assert_called_once_with(None)


@pytest.mark.redis
@pytest.mark.asyncio
class TestConsumeStream:
    def get_broker(self, apply_types: bool = False):
        return RedisBroker(apply_types=apply_types)

    def patch_broker(self, broker):
        return broker

    @pytest.mark.slow
    async def test_consume_stream(
        self,
        event: asyncio.Event,
        mock: MagicMock,
        queue,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber(stream=StreamSub(queue, polling_interval=10))
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

    @pytest.mark.slow
    async def test_consume_stream_with_big_interval(
        self,
        event: asyncio.Event,
        mock: MagicMock,
        queue,
    ):
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

    @pytest.mark.slow
    async def test_consume_stream_native(
        self,
        event: asyncio.Event,
        mock: MagicMock,
        queue,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber(stream=StreamSub(queue, polling_interval=10))
        async def handler(msg):
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br._connection.xadd(queue, {"message": "hello"})
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with({"message": "hello"})

    @pytest.mark.slow
    async def test_consume_stream_batch(
        self,
        event: asyncio.Event,
        mock: MagicMock,
        queue,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(queue, polling_interval=10, batch=True)
        )
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

        mock.assert_called_once_with(["hello"])

    @pytest.mark.slow
    async def test_consume_stream_batch_headers(
        self,
        queue: str,
        event: asyncio.Event,
        mock,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(queue, polling_interval=10, batch=True)
        )
        def subscriber(m, msg: RedisMessage):
            check = all(
                (
                    msg.headers,
                    msg.headers["correlation_id"]
                    == msg.batch_headers[0]["correlation_id"],
                    msg.headers.get("custom") == "1",
                )
            )
            mock(check)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish("", stream=queue, headers={"custom": "1"})
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()
            mock.assert_called_once_with(True)

    @pytest.mark.slow
    async def test_consume_stream_batch_complex(
        self,
        queue,
    ):
        consume_broker = self.get_broker(apply_types=True)

        from pydantic import BaseModel

        class Data(BaseModel):
            m: str

        msgs_queue = asyncio.Queue(maxsize=1)

        @consume_broker.subscriber(
            stream=StreamSub(queue, polling_interval=10, batch=True)
        )
        async def handler(msg: List[Data]):
            await msgs_queue.put(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br.publish(Data(m="hi"), stream=queue)

            result, _ = await asyncio.wait(
                (asyncio.create_task(msgs_queue.get()),),
                timeout=3,
            )

        assert next(iter(result)).result() == [Data(m="hi")]

    @pytest.mark.slow
    async def test_consume_stream_batch_native(
        self,
        event: asyncio.Event,
        mock: MagicMock,
        queue,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(queue, polling_interval=10, batch=True)
        )
        async def handler(msg):
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br._connection.xadd(queue, {"message": "hello"})
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with([{"message": "hello"}])

    async def test_consume_group(
        self,
        queue: str,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue)
        )
        async def handler(msg: RedisMessage): ...

        assert next(iter(consume_broker._subscribers.values())).last_id == "$"

    async def test_consume_group_with_last_id(
        self,
        queue: str,
    ):
        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue, last_id="0")
        )
        async def handler(msg: RedisMessage): ...

        assert next(iter(consume_broker._subscribers.values())).last_id == "0"

    async def test_consume_nack(
        self,
        queue: str,
        event: asyncio.Event,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue)
        )
        async def handler(msg: RedisMessage):
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
        event: asyncio.Event,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue)
        )
        async def handler(msg: RedisMessage):
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

    async def test_consume_and_delete_acked(self, queue: str, event: asyncio.Event):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue)
        )
        async def handler(msg: RedisStreamMessage):
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
                    timeout=300,
                )

                m.mock.assert_called_once()

            queue_len = await br._connection.xlen(queue)
            assert queue_len == 0, (
                f"Redis stream must be empty here, found {queue_len} messages"
            )
        assert event.is_set()

    async def test_consume_and_delete_nacked(self, queue: str, event: asyncio.Event):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(queue, group="group", consumer=queue),
            no_ack=True,
        )
        async def handler(msg: RedisStreamMessage):
            event.set()
            assert not msg.committed
            await msg.delete(consume_broker._connection)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(Redis, "xdel", spy_decorator(Redis.xdel)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", stream=queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=300,
                )

                m.mock.assert_called_once()

            queue_len = await br._connection.xlen(queue)
            assert queue_len == 0, (
                f"Redis stream must be empty here, found {queue_len} messages"
            )
        assert event.is_set()

    async def test_get_one(
        self,
        queue: str,
    ):
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(stream=queue)

        async with self.patch_broker(broker) as br:
            await br.start()

            message = None

            async def consume():
                nonlocal message
                message = await subscriber.get_one(timeout=3)

            async def publish():
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
    ):
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(stream=queue)

        async with self.patch_broker(broker) as br:
            await br.start()

            mock(await subscriber.get_one(timeout=1e-24))
            mock.assert_called_once_with(None)
