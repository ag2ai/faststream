import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream import Context
from faststream.confluent import KafkaPublishMessage, KafkaResponse
from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import ConfluentTestcaseConfig


@pytest.mark.connected()
@pytest.mark.confluent()
class TestPublish(ConfluentTestcaseConfig, BrokerPublishTestcase):
    @pytest.mark.asyncio()
    async def test_publish_batch(self, queue: str) -> None:
        pub_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=2)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(msg) -> None:
            await msgs_queue.put(msg)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await br.publish_batch(1, "hi", topic=queue)

            result, _ = await asyncio.wait(
                (
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                ),
                timeout=self.timeout,
            )

        assert {1, "hi"} == {r.result() for r in result}

    @pytest.mark.asyncio()
    async def test_batch_publisher_manual(self, queue: str) -> None:
        pub_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=2)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
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
                timeout=self.timeout,
            )

        assert {1, "hi"} == {r.result() for r in result}

    @pytest.mark.asyncio()
    async def test_batch_publisher_decorator(self, queue: str) -> None:
        pub_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=2)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(msg) -> None:
            await msgs_queue.put(msg)

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @pub_broker.publisher(queue, batch=True)
        @pub_broker.subscriber(*args2, **kwargs2)
        async def pub(m):
            return 1, "hi"

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await br.publish("", queue + "1")

            result, _ = await asyncio.wait(
                (
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                ),
                timeout=self.timeout,
            )

        assert {1, "hi"} == {r.result() for r in result}

    @pytest.mark.asyncio()
    async def test_response(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        @pub_broker.publisher(topic=queue + "1")
        async def handle():
            return KafkaResponse(1)

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @pub_broker.subscriber(*args2, **kwargs2)
        async def handle_next(msg=Context("message")) -> None:
            mock(body=msg.body)
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout * 1.5,
            )

        assert event.is_set()
        mock.assert_called_once_with(body=b"1")

    @pytest.mark.asyncio()
    async def test_batch_publisher_manual_with_key(self, queue: str) -> None:
        pub_broker = self.get_broker(apply_types=True)

        messages_queue: asyncio.Queue[tuple[Any, bytes | None]] = asyncio.Queue()

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(msg: Any, raw_msg=Context("message")) -> None:
            await messages_queue.put((msg, raw_msg.raw_message.key()))

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

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(msg: Any, raw_msg=Context("message")) -> None:
            await messages_queue.put((msg, raw_msg.raw_message.key()))

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

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(msg: Any, raw_msg=Context("message")) -> None:
            await messages_queue.put((msg, raw_msg.raw_message.key()))

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

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(msg: Any, raw_msg=Context("message")) -> None:
            await messages_queue.put((msg, raw_msg.raw_message.key()))

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
