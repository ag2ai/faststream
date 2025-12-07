import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from aiokafka.structs import RecordMetadata

from faststream import Context
from faststream.kafka import KafkaPublishMessage, KafkaResponse
from faststream.kafka.exceptions import BatchBufferOverflowException
from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.connected()
@pytest.mark.flaky(reruns=3, reruns_delay=1)
class TestPublish(KafkaTestcaseConfig, BrokerPublishTestcase):
    @pytest.mark.asyncio()
    async def test_publish_batch(self, queue: str) -> None:
        pub_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=2)

        @pub_broker.subscriber(queue)
        async def handler(msg) -> None:
            await msgs_queue.put(msg)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            record_metadata = await br.publish_batch(1, "hi", topic=queue)
            result, _ = await asyncio.wait(
                (
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                ),
                timeout=3,
            )
            assert isinstance(record_metadata, RecordMetadata)

        assert {1, "hi"} == {r.result() for r in result}

    @pytest.mark.asyncio()
    async def test_batch_publisher_manual(self, queue: str) -> None:
        pub_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=2)

        @pub_broker.subscriber(queue)
        async def handler(msg) -> None:
            await msgs_queue.put(msg)

        publisher = pub_broker.publisher(queue, batch=True)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await publisher.publish(1, "hi")

            result, _ = await asyncio.wait(
                (
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                ),
                timeout=3,
            )

        assert {1, "hi"} == {r.result() for r in result}

    @pytest.mark.asyncio()
    async def test_batch_publisher_decorator(self, queue: str) -> None:
        pub_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=2)

        @pub_broker.subscriber(queue)
        async def handler(msg) -> None:
            await msgs_queue.put(msg)

        @pub_broker.publisher(queue, batch=True)
        @pub_broker.subscriber(queue + "1")
        async def pub(m):
            return 1, "hi"

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            record_metadata = await br.publish("", queue + "1")

            result, _ = await asyncio.wait(
                (
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                ),
                timeout=3,
            )
            assert isinstance(record_metadata, RecordMetadata)

        assert {1, "hi"} == {r.result() for r in result}

    @pytest.mark.asyncio()
    async def test_response(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue)
        @pub_broker.publisher(queue + "1")
        async def handle():
            return KafkaResponse(1, key=b"1")

        @pub_broker.subscriber(queue + "1")
        async def handle_next(msg=Context("message")) -> None:
            mock(
                body=msg.body,
                key=msg.raw_message.key,
            )
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(
            body=b"1",
            key=b"1",
        )

    @pytest.mark.asyncio()
    async def test_return_future(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        pub_broker = self.get_broker()

        @pub_broker.subscriber(queue)
        async def handler(m) -> None:
            pass

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            batch_record_metadata_future = await br.publish_batch(
                1,
                "hi",
                topic=queue,
                no_confirm=True,
            )
            record_metadata_future = await br.publish("", topic=queue, no_confirm=True)
            assert isinstance(batch_record_metadata_future, asyncio.Future)
            assert isinstance(record_metadata_future, asyncio.Future)

    @pytest.mark.asyncio()
    async def test_raise_buffer_overflow_exception(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        pub_broker = self.get_broker(max_batch_size=16)

        @pub_broker.subscriber(queue)
        async def handler(m) -> None:
            pass

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            with pytest.raises(BatchBufferOverflowException) as e:
                await br.publish_batch(1, "Hello, world!", topic=queue, no_confirm=True)
            assert e.value.message_position == 1

    @pytest.mark.asyncio()
    async def test_can_explicitly_publish_on_partition_0_from_publisher(
        self,
        queue: str,
    ) -> None:
        pub_broker = self.get_broker()

        with patch(
            "aiokafka.AIOKafkaProducer.send",
            autospec=True,
        ) as producer_send_mock:
            async with self.patch_broker(pub_broker) as br:
                publisher = br.publisher(queue)
                await br.start()
                await publisher.publish("Hello, world!", partition=0, no_confirm=True)

        producer_send_mock.assert_called_once()
        assert producer_send_mock.mock_calls[0].kwargs["partition"] == 0

    @pytest.mark.asyncio()
    async def test_batch_publisher_manual_with_key(self, queue: str) -> None:
        pub_broker = self.get_broker(apply_types=True)

        messages_queue: asyncio.Queue[tuple[Any, bytes | None]] = asyncio.Queue()

        @pub_broker.subscriber(queue)
        async def handler(msg: Any, raw_msg=Context("message")) -> None:
            await messages_queue.put((msg, raw_msg.raw_message.key))

        publisher = pub_broker.publisher(queue, batch=True)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await publisher.publish(1, "hi", key=b"my_key")

            received = []
            for _ in range(2):
                msg, key = await asyncio.wait_for(
                    messages_queue.get(), timeout=self.timeout
                )
                received.append((msg, key))

        assert len(received) == 2
        assert all(key == b"my_key" for _, key in received)

        bodies = {msg for msg, _ in received}
        assert bodies == {1, "hi"}

        assert messages_queue.empty()

    @pytest.mark.asyncio()
    async def test_batch_publisher_default_key_from_factory(self, queue: str) -> None:
        pub_broker = self.get_broker(apply_types=True)

        messages_queue: asyncio.Queue[tuple[Any, bytes | None]] = asyncio.Queue()

        @pub_broker.subscriber(queue)
        async def handler(msg: Any, raw_msg=Context("message")) -> None:
            await messages_queue.put((msg, raw_msg.raw_message.key))

        publisher = pub_broker.publisher(queue, batch=True, key=b"default_key")

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await publisher.publish(1, "hi")

            received = []
            for _ in range(2):
                msg, key = await asyncio.wait_for(
                    messages_queue.get(), timeout=self.timeout
                )
                received.append((msg, key))

        assert len(received) == 2
        assert all(key == b"default_key" for _, key in received)

        bodies = {msg for msg, _ in received}
        assert bodies == {1, "hi"}

        assert messages_queue.empty()

    @pytest.mark.asyncio()
    async def test_batch_publisher_with_kafka_response_per_message_keys(
        self, queue: str
    ) -> None:
        """Test using KafkaResponse to set different keys for each message."""
        pub_broker = self.get_broker(apply_types=True)

        messages_queue: asyncio.Queue[tuple[Any, bytes | None]] = asyncio.Queue()

        @pub_broker.subscriber(queue)
        async def handler(msg: Any, raw_msg=Context("message")) -> None:
            await messages_queue.put((msg, raw_msg.raw_message.key))

        publisher = pub_broker.publisher(queue, batch=True)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await publisher.publish(
                KafkaPublishMessage("message1", key=b"key1"),
                KafkaPublishMessage("message2", key=b"key2"),
                "message3",
            )

            received = []
            for _ in range(3):
                msg, key = await asyncio.wait_for(
                    messages_queue.get(), timeout=self.timeout
                )
                received.append((msg, key))

        received_set = set(received)
        expected_set = {
            ("message1", b"key1"),
            ("message2", b"key2"),
            ("message3", None),
        }
        assert received_set == expected_set, (
            f"Expected {expected_set}, got {received_set}"
        )

        assert messages_queue.empty()

    @pytest.mark.asyncio()
    async def test_batch_publisher_kafka_response_with_default_key(
        self, queue: str
    ) -> None:
        """Test KafkaResponse with publisher default key fallback."""
        pub_broker = self.get_broker(apply_types=True)

        messages_queue: asyncio.Queue[tuple[Any, bytes | None]] = asyncio.Queue()

        @pub_broker.subscriber(queue)
        async def handler(msg: Any, raw_msg=Context("message")) -> None:
            await messages_queue.put((msg, raw_msg.raw_message.key))

        publisher = pub_broker.publisher(queue, batch=True, key=b"default_key")

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await publisher.publish(
                KafkaResponse("message1", key=b"explicit_key"),
                "message2",
                KafkaResponse("message3", key=b"another_key"),
            )

            received = []
            for _ in range(3):
                msg, key = await asyncio.wait_for(
                    messages_queue.get(), timeout=self.timeout
                )
                received.append((msg, key))

        received_set = set(received)
        expected_set = {
            ("message1", b"explicit_key"),
            ("message2", b"default_key"),
            ("message3", b"another_key"),
        }
        assert received_set == expected_set, (
            f"Expected {expected_set}, got {received_set}"
        )

        assert messages_queue.empty()
