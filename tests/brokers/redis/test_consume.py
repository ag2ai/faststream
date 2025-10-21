import asyncio
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
from faststream.redis.subscriber.usecases.stream_subscriber import (
    RedisConsumerReader,
    RedisGroupReader,
    RedisStreamReader,
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
    async def test_stream_reader(self, queue: str) -> None:
        """Should configure the reader to start from '$'.

        The reader, when configured for polling will attempt to read
        the next available message that comes in and then proceed
        incrementally.
        """
        broker = self.get_broker()
        client = await broker.connect()

        stream = StreamSub(queue, polling_interval=100)
        reader = RedisStreamReader(stream_sub=stream)

        # ignored, occurred before polling
        await client.xadd(queue, {"message": "1"})

        await reader.configure(client=client, prefix="")
        assert reader.client == client
        assert reader._last_id == "$"

        response = await reader()
        assert not response

        response, id2 = await asyncio.gather(
            reader(),
            client.xadd(queue, {"message": "2"}),
        )

        assert len(response) == 1
        assert response[0][1][0][0] == id2
        assert reader._last_id == id2.decode()

        id3 = await client.xadd(queue, {"message": "3"})
        response = await reader()
        assert response[0][1][0][0] == id3
        assert reader._last_id == id3.decode()

    @pytest.mark.slow()
    async def test_stream_group_reader(self, queue: str) -> None:
        """Should configure the reader to read unread messages.

        The reader will start reading from the first unread message
        in the stream and then continue using the redis stream
        consumer's internal offset.
        """
        broker = self.get_broker()
        client = await broker.connect()

        stream = StreamSub(queue, group=queue, consumer=queue)
        reader = RedisGroupReader(stream_sub=stream)

        # ignored the consumer group is created in the .configure method
        await client.xadd(queue, {"message": "1"})

        await reader.configure(client=client, prefix="")

        id2 = await client.xadd(queue, {"message": "2"})
        id3 = await client.xadd(queue, {"message": "3"})

        response = await reader()
        assert len(response) == 1
        assert response[0][1][0][0] == id2

        response = await reader()
        assert response[0][1][0][0] == id3

    @pytest.mark.slow()
    async def test_stream_group_reader_pending(self, queue: str) -> None:
        """Should configure the reader to read pending messages.

        With an offset configured, the reader will only read from the
        pending entries list (PEL) using the redis stream
        consumer's internal offset.

        In order to progress to the next message, it must be acknowledged.
        """
        broker = self.get_broker()
        client = await broker.connect()

        stream = StreamSub(queue, group=queue, consumer=queue, last_id="0")
        reader = RedisGroupReader(stream_sub=stream)

        await reader.configure(client=client, prefix="")

        id1 = await client.xadd(queue, {"message": "1"})
        id2 = await client.xadd(queue, {"message": "2"})

        # read and do not acknowledge messages 1 and 2
        await asyncio.gather(reader.read(">"), reader.read(">"))

        response = await reader()
        assert len(response) == 1
        assert response[0][1][0][0] == id1

        response = await reader()
        assert response[0][1][0][0] == id1

        await client.xack(queue, queue, id1)  # type: ignore[no-untyped-call]

        response = await reader()
        assert response[0][1][0][0] == id2

    @pytest.mark.slow()
    async def test_stream_consumer_reader(self, queue: str) -> None:
        """Should configure the reader to read unread messages by default.

        When resumable is disabled, the stream consumer reader behaves
        much like the group reader and will read from either pending
        or new messages.
        """
        broker = self.get_broker()
        client = await broker.connect()

        stream = StreamSub(queue, group=queue, consumer=queue)
        reader = RedisConsumerReader(stream_sub=stream)

        await client.xadd(queue, {"message": "1"})

        await reader.configure(client=client, prefix="")

        id2 = await client.xadd(queue, {"message": "2"})
        id3 = await client.xadd(queue, {"message": "3"})

        response = await reader()
        assert len(response) == 1
        assert response[0][1][0][0] == id2

        response = await reader()
        assert len(response) == 1
        assert response[0][1][0][0] == id3

    @pytest.mark.slow()
    async def test_stream_consumer_reader_resumable(self, queue: str) -> None:
        """Should configure the reader to read pending and then new messages.

        When resumable is enabled, the reader will consume from the pending
        queue until no pending messages remain and then continue with new
        messages.
        """
        broker = self.get_broker()
        client = await broker.connect()

        stream = StreamSub(queue, group=queue, consumer=queue)
        reader = RedisConsumerReader(stream_sub=stream)

        await client.xadd(queue, {"message": "1"})

        await reader.configure(client=client, prefix="")

        id2 = await client.xadd(queue, {"message": "2"})
        id3 = await client.xadd(queue, {"message": "3"})

        await reader.read(">")
        reader.resume_from("0", resumable=True)

        response = await reader()
        assert len(response) == 1
        assert response[0][1][0][0] == id2
        assert reader._resume_from == "0"
        await client.xack(queue, queue, id2)  # type: ignore[no-untyped-call]

        response = await reader()
        assert len(response) == 1
        assert response[0][1][0][0] == id3
        # once no pending messages remain, resume_from is cleared
        assert reader._resume_from is None

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

    async def test_consume_pending_with_group(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        """Should first attempt to consume from PEL then from stream."""
        event = asyncio.Event()

        consume_broker = self.get_broker()
        client = await consume_broker.connect()

        stream = StreamSub(queue, group="group", consumer=queue, last_id="0")
        subscriber = consume_broker.subscriber(stream=stream)

        @subscriber
        async def handler(msg: RedisMessage) -> None:
            mock(msg)
            if mock.call_count == 2:
                event.set()

        async with self.patch_broker(consume_broker) as br:
            await client.xgroup_create(
                name=queue,
                id="$",
                groupname="group",
                mkstream=True,
            )

            ignored = await client.xadd(queue, {"message": "ignored"})
            await client.xadd(queue, {"message": "pending"})
            await client.xadd(queue, {"message": "new"})

            # consume the "pending" message
            await client.xreadgroup(
                groupname="group",
                consumername=queue,
                count=1,
                streams={queue: ">"},
            )

            # acknowledge the first message, it should be ignored
            await client.xack(queue, "group", ignored)  # type: ignore[no-untyped-call]

            await br.start()

            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=3,
            )

        mock.assert_has_calls([
            call({"message": "pending"}),
            call({"message": "new"}),
        ])

        assert mock.call_count == 2

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
