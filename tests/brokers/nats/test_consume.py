import asyncio
from unittest.mock import MagicMock, patch

import pytest
from nats.aio.msg import Msg

from faststream import AckPolicy
from faststream.exceptions import AckMessage
from faststream.nats import ConsumerConfig, JStream, PubAck, PullSub
from faststream.nats.annotations import NatsMessage
from faststream.nats.message import NatsMessage as StreamMessage
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator

from .basic import NatsTestcaseConfig


@pytest.mark.connected()
@pytest.mark.nats()
class TestConsume(NatsTestcaseConfig, BrokerRealConsumeTestcase):
    async def test_concurrent_subscriber(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        event2 = asyncio.Event()

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue, max_workers=2)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg):
            mock()

            if event.is_set():
                event2.set()
            else:
                event.set()

            await asyncio.sleep(1.0)

        async with self.patch_broker(broker) as br:
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

    async def test_consume_js(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue, stream=stream)

        @consume_broker.subscriber(*args, **kwargs)
        def subscriber(m) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            result = await br.publish("hello", queue, stream=stream.name)

            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=3,
            )

            assert isinstance(result, PubAck), result
        assert event.is_set()

    async def test_consume_with_filter(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            config=ConsumerConfig(filter_subjects=[f"{queue}.a"]),
            stream=JStream(queue, subjects=[f"{queue}.*"]),
        )
        def subscriber(m) -> None:
            mock(m)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(2, f"{queue}.a")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(2)

    async def test_consume_pull(
        self,
        queue: str,
        stream: JStream,
        mock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1),
        )
        def subscriber(m) -> None:
            mock(m)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()
            mock.assert_called_once_with("hello")

    async def test_consume_batch(
        self,
        queue: str,
        stream: JStream,
        mock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True),
        )
        def subscriber(m) -> None:
            mock(m)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(b"hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()
            mock.assert_called_once_with([b"hello"])

    async def test_core_consume_no_ack(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            ack_policy=AckPolicy.MANUAL,
        )

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg: NatsMessage) -> None:
            mock(msg.raw_message._ackd)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # Check, that Core Subscriber doesn't call Acknowledgement automatically
            with patch.object(
                StreamMessage,
                "ack",
                spy_decorator(StreamMessage.ack),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                assert not m.mock.called

        assert event.is_set()
        mock.assert_called_once_with(True)  # True was set by parser

    async def test_consume_ack(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue, stream=stream)
        async def handler(msg: NatsMessage) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(Msg, "ack", spy_decorator(Msg.ack)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    async def test_consume_ack_manual(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue, stream=stream)
        async def handler(msg: NatsMessage) -> None:
            await msg.ack()
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(Msg, "ack", spy_decorator(Msg.ack)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    async def test_consume_ack_sync_manual(
        self,
        queue: str,
        event: asyncio.Event,
        stream: JStream,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue, stream=stream)
        async def handler(msg: NatsMessage):
            await msg.ack_sync()
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(Msg, "ack_sync", spy_decorator(Msg.ack_sync)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    async def test_consume_ack_raise(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue, stream=stream)
        async def handler(msg: NatsMessage):
            event.set()
            raise AckMessage

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(Msg, "ack", spy_decorator(Msg.ack)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    async def test_nack(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue, stream=stream)
        async def handler(msg: NatsMessage) -> None:
            await msg.nack()
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(Msg, "nak", spy_decorator(Msg.nak)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    async def test_consume_no_ack(
        self,
        queue: str,
        stream: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue,
            stream=stream,
            ack_policy=AckPolicy.MANUAL,
        )
        async def handler(msg: NatsMessage) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(Msg, "ack", spy_decorator(Msg.ack)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_not_called()

            assert event.is_set()

    async def test_consume_batch_headers(
        self,
        queue: str,
        stream: JStream,
        mock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True),
        )
        def subscriber(m, msg: NatsMessage) -> None:
            check = all(
                (
                    msg.headers,
                    [msg.headers] == msg.batch_headers,
                    msg.headers.get("custom") == "1",
                ),
            )
            mock(check)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue, headers={"custom": "1"})),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(True)

    @pytest.mark.asyncio()
    async def test_consume_kv(
        self,
        queue: str,
        mock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue, kv_watch=queue + "1")
        async def handler(m) -> None:
            mock(m)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            bucket = await br.key_value(queue + "1")

            await asyncio.wait(
                (
                    asyncio.create_task(
                        bucket.put(
                            queue,
                            b"world",
                        ),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_with(b"world")

    @pytest.mark.asyncio()
    async def test_consume_os(
        self,
        queue: str,
        mock,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue, obj_watch=True)
        async def handler(filename: str) -> None:
            event.set()
            mock(filename)

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            bucket = await br.object_storage(queue)

            await asyncio.wait(
                (
                    asyncio.create_task(
                        bucket.put(
                            "hello",
                            b"world",
                        ),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with("hello")

    async def test_get_one_js(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(queue, stream=stream)

        async with self.patch_broker(broker) as br:
            await br.start()

            message = None

            async def consume() -> None:
                nonlocal message
                message = await subscriber.get_one(timeout=5)

            async def publish() -> None:
                await br.publish("test_message", queue, stream=stream.name)

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish()),
                ),
                timeout=10,
            )

            assert message is not None
            assert await message.decode() == "test_message"

    async def test_get_one_timeout_js(
        self,
        queue: str,
        stream: JStream,
        mock,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(queue, stream=stream)

        async with self.patch_broker(broker) as br:
            await br.start()

            mock(await subscriber.get_one(timeout=1e-24))
            mock.assert_called_once_with(None)

    async def test_get_one_pull(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1),
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            message = None

            async def consume() -> None:
                nonlocal message
                message = await subscriber.get_one(timeout=5)

            async def publish() -> None:
                await br.publish("test_message", queue)

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish()),
                ),
                timeout=10,
            )

            assert message is not None
            assert await message.decode() == "test_message"

    async def test_get_one_pull_timeout(
        self,
        queue: str,
        stream: JStream,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1),
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            mock(await subscriber.get_one(timeout=1e-24))
            mock.assert_called_once_with(None)

    async def test_get_one_batch(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True),
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            message = None

            async def consume() -> None:
                nonlocal message
                message = await subscriber.get_one(timeout=5)

            async def publish() -> None:
                await br.publish("test_message", queue)

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish()),
                ),
                timeout=10,
            )

            assert message is not None
            assert await message.decode() == ["test_message"]

    async def test_get_one_batch_timeout(
        self,
        queue: str,
        stream: JStream,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True),
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            mock(await subscriber.get_one(timeout=1e-24))
            mock.assert_called_once_with(None)

    async def test_get_one_with_filter(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(
            config=ConsumerConfig(filter_subjects=[f"{queue}.a"]),
            stream=JStream(queue, subjects=[f"{queue}.*"]),
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            message = None

            async def consume() -> None:
                nonlocal message
                message = await subscriber.get_one(timeout=5)

            async def publish() -> None:
                await br.publish("test_message", f"{queue}.a")

            await asyncio.wait(
                (
                    asyncio.create_task(publish()),
                    asyncio.create_task(consume()),
                ),
                timeout=10,
            )

            assert message is not None
            assert await message.decode() == "test_message"

    async def test_get_one_kv(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(queue, kv_watch=queue + "1")

        async with self.patch_broker(broker) as br:
            await br.start()
            bucket = await br.key_value(queue + "1")

            message = None

            async def consume() -> None:
                nonlocal message
                message = await subscriber.get_one(timeout=5)

            async def publish() -> None:
                await bucket.put(queue, b"test_message")

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish()),
                ),
                timeout=10,
            )

            assert message is not None
            assert await message.decode() == b"test_message"

    async def test_get_one_kv_timeout(
        self,
        queue: str,
        stream: JStream,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(queue, kv_watch=queue + "1")

        async with self.patch_broker(broker) as br:
            await br.start()

            mock(await subscriber.get_one(timeout=1e-24))
            mock.assert_called_once_with(None)

    async def test_get_one_os(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(queue, obj_watch=True)

        async with self.patch_broker(broker) as br:
            await br.start()
            bucket = await br.object_storage(queue)

            new_object_id = None

            async def consume() -> None:
                nonlocal new_object_id
                new_object_event = await subscriber.get_one(timeout=5)
                new_object_id = await new_object_event.decode()

            async def publish() -> None:
                await bucket.put(queue, b"test_message")

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish()),
                ),
                timeout=10,
            )

            new_object = await bucket.get(new_object_id)
            assert new_object.data == b"test_message"

    async def test_get_one_os_timeout(
        self,
        queue: str,
        stream: JStream,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(queue, obj_watch=True)

        async with self.patch_broker(broker) as br:
            await br.start()

            mock(await subscriber.get_one(timeout=1e-24))
            mock.assert_called_once_with(None)

    async def test_iterator_js(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        expected_messages = ("test_message_1", "test_message_2")

        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(queue, stream=stream)

        async with self.patch_broker(broker) as br:
            await br.start()

            async def publish_test_message():
                for msg in expected_messages:
                    await br.publish(msg, queue)

            _ = await asyncio.create_task(publish_test_message())

            index_message = 0
            async for msg in subscriber:
                result_message = await msg.decode()

                assert result_message == expected_messages[index_message]

                index_message += 1
                if index_message >= len(expected_messages):
                    break

    async def test_iterator_pull(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        expected_messages = ("test_message_1", "test_message_2")

        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1),
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            async def publish_test_message():
                for msg in expected_messages:
                    await br.publish(msg, queue)

            _ = await asyncio.create_task(publish_test_message())

            index_message = 0
            async for msg in subscriber:
                result_message = await msg.decode()

                assert result_message == expected_messages[index_message]

                index_message += 1
                if index_message >= len(expected_messages):
                    break

    async def test_iterator_batch(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        expected_messages = ("test_message_1", "test_message_2")

        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True),
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            async def publish_test_message():
                for msg in expected_messages:
                    await br.publish(msg, queue)

            _ = await asyncio.create_task(publish_test_message())

            index_message = 0
            async for msg in subscriber:
                result_message = await msg.decode()

                assert result_message == [expected_messages[index_message]]

                index_message += 1
                if index_message >= len(expected_messages):
                    break

    async def test_iterator_with_filter(
        self,
        queue: str,
    ) -> None:
        expected_messages = ("test_message_1", "test_message_2")

        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(
            config=ConsumerConfig(filter_subjects=[f"{queue}.a"]),
            stream=JStream(queue, subjects=[f"{queue}.*"]),
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            async def publish_test_message():
                for msg in expected_messages:
                    await br.publish(msg, f"{queue}.a")

            _ = await asyncio.create_task(publish_test_message())

            index_message = 0
            async for msg in subscriber:
                result_message = await msg.decode()

                assert result_message == expected_messages[index_message]

                index_message += 1
                if index_message >= len(expected_messages):
                    break

    async def test_iterator_kv(
        self,
        queue: str,
    ) -> None:
        expected_messages = (b"test_message_1", b"test_message_2")

        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(queue, kv_watch=queue + "1")

        async with self.patch_broker(broker) as br:
            await br.start()
            bucket = await br.key_value(queue + "1")

            async def publish_test_message():
                await bucket.put(queue, expected_messages[0])

            _ = await asyncio.create_task(publish_test_message())

            index_message = 0
            async for msg in subscriber:
                result_message = await msg.decode()

                assert result_message == expected_messages[index_message]

                index_message += 1
                if index_message >= len(expected_messages):
                    break

                await bucket.put(queue, expected_messages[index_message])

    async def test_iterator_os(
        self,
        queue: str,
    ) -> None:
        expected_messages = (b"test_message_1", b"test_message_2")

        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(queue, obj_watch=True)

        async with self.patch_broker(broker) as br:
            await br.start()
            bucket = await br.object_storage(queue)

            async def publish_test_message():
                await bucket.put(queue, expected_messages[0])

            _ = await asyncio.create_task(publish_test_message())

            index_message = 0
            async for new_object_event in subscriber:
                new_object_id = await new_object_event.decode()
                new_object = await bucket.get(new_object_id)

                assert new_object.data == expected_messages[index_message]

                index_message += 1
                if index_message >= len(expected_messages):
                    break

                await bucket.put(queue, expected_messages[index_message])
