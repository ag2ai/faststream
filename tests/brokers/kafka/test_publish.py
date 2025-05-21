import asyncio
from unittest.mock import Mock

import pytest
from aiokafka.structs import RecordMetadata

from faststream import Context
from faststream.kafka import KafkaResponse
from faststream.kafka.exceptions import BatchBufferOverflowException
from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
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
        mock: Mock,
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
        mock: Mock,
    ) -> None:
        pub_broker = self.get_broker()

        @pub_broker.subscriber(queue)
        async def handler(m) -> None:
            pass

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            batch_record_metadata_future = await br.publish_batch(
                1, "hi", topic=queue, no_confirm=True
            )
            record_metadata_future = await br.publish("", topic=queue, no_confirm=True)
            assert isinstance(batch_record_metadata_future, asyncio.Future)
            assert isinstance(record_metadata_future, asyncio.Future)

    @pytest.mark.asyncio()
    async def test_raise_buffer_overflow_exception(
        self, queue: str, mock: Mock
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
