"""Tests for SQS-specific behaviour added for broker parity.

Covers producer guards (batch chunking, attribute/size limits, FIFO validation),
parser typing/system-attributes, configurable nack and batch consumption.
"""

import asyncio
from unittest.mock import AsyncMock

import pytest

from faststream import Context
from faststream.exceptions import SetupError
from faststream.response.publish_type import PublishType
from faststream.sqs import FifoQueue
from faststream.sqs.exceptions import (
    MAX_MESSAGE_SIZE,
    FifoQueueError,
    MessageTooLargeError,
    TooManyMessageAttributesError,
)
from faststream.sqs.message import SQSBatchMessage, SQSMessage
from faststream.sqs.parser import SQSParser
from faststream.sqs.publisher.producer import SQSFastProducer
from faststream.sqs.response import SQSPublishCommand

from .basic import SQSMemoryTestcaseConfig, SQSTestcaseConfig


def _attr(value: str) -> dict[str, str]:
    return {"DataType": "String", "StringValue": value}


class TestProducerGuards:
    def test_chunk_entries_splits_over_ten(self) -> None:
        entries = [{"MessageBody": "x", "MessageAttributes": {}} for _ in range(23)]
        chunks = list(SQSFastProducer._chunk_entries(entries))
        assert [len(c) for c in chunks] == [10, 10, 3]

    def test_chunk_entries_splits_on_size(self) -> None:
        big = "a" * (MAX_MESSAGE_SIZE // 2 + 1)
        entries = [{"MessageBody": big, "MessageAttributes": {}} for _ in range(3)]
        chunks = list(SQSFastProducer._chunk_entries(entries))
        # Each entry is >half the limit, so no two fit together.
        assert all(len(c) == 1 for c in chunks)
        assert len(chunks) == 3

    def test_too_many_attributes(self) -> None:
        producer = SQSFastProducer()
        attrs = {f"h{i}": _attr("v") for i in range(11)}
        with pytest.raises(TooManyMessageAttributesError):
            producer._validate_message("body", attrs)

    def test_message_too_large(self) -> None:
        producer = SQSFastProducer()
        body = "a" * (MAX_MESSAGE_SIZE + 1)
        with pytest.raises(MessageTooLargeError):
            producer._validate_message(body, {})

    def test_fifo_requires_group_id(self) -> None:
        cmd = SQSPublishCommand(
            "data",
            queue="orders.fifo",
            _publish_type=PublishType.PUBLISH,
        )
        with pytest.raises(FifoQueueError):
            SQSFastProducer._validate_fifo(cmd)

    def test_fifo_ok_with_group_id(self) -> None:
        cmd = SQSPublishCommand(
            "data",
            queue="orders.fifo",
            group_id="g1",
            _publish_type=PublishType.PUBLISH,
        )
        # Should not raise.
        SQSFastProducer._validate_fifo(cmd)


class TestRequestAttemptId(SQSMemoryTestcaseConfig):
    def test_passed_to_receive_kwargs(self) -> None:
        broker = self.get_broker()
        fifo = FifoQueue(name="orders")

        sub = broker.subscriber(fifo, request_attempt_id="attempt-1")

        assert sub._receive_kwargs()["ReceiveRequestAttemptId"] == "attempt-1"

    def test_absent_by_default(self) -> None:
        broker = self.get_broker()
        fifo = FifoQueue(name="orders")

        sub = broker.subscriber(fifo)

        assert "ReceiveRequestAttemptId" not in sub._receive_kwargs()

    def test_rejected_for_non_fifo_queue(self) -> None:
        broker = self.get_broker()
        with pytest.raises(SetupError):
            broker.subscriber("plain-queue", request_attempt_id="attempt-1")


@pytest.mark.asyncio()
class TestParser:
    async def test_binary_attribute_decoded_as_bytes(self) -> None:
        parser = SQSParser()
        raw = {
            "Body": "hello",
            "MessageAttributes": {
                "blob": {"DataType": "Binary", "BinaryValue": b"\x00\x01"},
                "num": {"DataType": "Number", "StringValue": "42"},
            },
        }
        msg = await parser.parse_message(raw)
        assert msg.headers["blob"] == b"\x00\x01"
        assert msg.headers["num"] == "42"

    async def test_system_attributes_exposed(self) -> None:
        parser = SQSParser()
        raw = {
            "Body": "x",
            "Attributes": {
                "ApproximateReceiveCount": "3",
                "MessageGroupId": "g1",
                "SequenceNumber": "1234",
                "SentTimestamp": "1700000000000",
            },
        }
        msg = await parser.parse_message(raw)
        assert msg.approximate_receive_count == 3
        assert msg.group_id == "g1"
        assert msg.sequence_number == "1234"
        assert msg.sent_timestamp == 1700000000000

    async def test_empty_receive_count_defaults_to_zero(self) -> None:
        parser = SQSParser()
        msg = await parser.parse_message({"Body": "x"})
        assert msg.approximate_receive_count == 0


@pytest.mark.asyncio()
class TestMessageAck:
    async def test_nack_default_immediate(self) -> None:
        client = AsyncMock()
        msg = SQSMessage(raw_message={"ReceiptHandle": "rh"}, body=b"")
        msg.sqs_client = client
        msg.queue_url = "url"
        await msg.nack()
        client.change_message_visibility.assert_awaited_once_with(
            QueueUrl="url", ReceiptHandle="rh", VisibilityTimeout=0
        )

    async def test_nack_with_backoff(self) -> None:
        client = AsyncMock()
        msg = SQSMessage(raw_message={"ReceiptHandle": "rh"}, body=b"")
        msg.sqs_client = client
        msg.queue_url = "url"
        await msg.nack(visibility_timeout=30)
        client.change_message_visibility.assert_awaited_once_with(
            QueueUrl="url", ReceiptHandle="rh", VisibilityTimeout=30
        )

    async def test_batch_ack_deletes_all(self) -> None:
        client = AsyncMock()
        raw = [{"ReceiptHandle": f"rh{i}"} for i in range(3)]
        msg = SQSBatchMessage(raw_message=raw, body=[b"", b"", b""])
        msg.sqs_client = client
        msg.queue_url = "url"
        await msg.ack()
        client.delete_message_batch.assert_awaited_once()
        _, kwargs = client.delete_message_batch.call_args
        assert {e["ReceiptHandle"] for e in kwargs["Entries"]} == {"rh0", "rh1", "rh2"}


@pytest.mark.asyncio()
class TestBatchConsume(SQSMemoryTestcaseConfig):
    async def test_batch_subscriber_receives_list(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, batch=True)
        async def handler(msg) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            handler.mock.assert_called_once_with(["hello"])


@pytest.mark.connected()
@pytest.mark.sqs()
@pytest.mark.asyncio()
class TestConnectedFeatures(SQSTestcaseConfig):
    async def test_batch_consume(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker(apply_types=True)
        received: list = []

        @broker.subscriber(queue, batch=True)
        async def handler(msgs: list) -> None:
            received.extend(msgs)
            # SQS does not guarantee returning all available messages in a single
            # ReceiveMessage call (ElasticMQ delivers them one at a time), so wait
            # until the whole batch has arrived across polls before asserting.
            if len(received) >= 3:
                event.set()

        async with broker:
            await broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish_batch("a", "b", "c", queue=queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert set(received) == {"a", "b", "c"}

    async def test_system_attributes_present(
        self, queue: str, event: asyncio.Event
    ) -> None:
        broker = self.get_broker(apply_types=True)
        counts: list[int] = []

        @broker.subscriber(queue)
        async def handler(msg: SQSMessage = Context("message")) -> None:
            counts.append(msg.approximate_receive_count)
            event.set()

        async with broker:
            await broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert counts
        assert counts[0] >= 1

    async def test_fifo_publish_consume(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker(apply_types=True)
        fifo = FifoQueue(name=f"{queue}.fifo", content_based_deduplication=True)
        received: list[str] = []

        @broker.subscriber(fifo)
        async def handler(msg: str) -> None:
            received.append(msg)
            event.set()

        async with broker:
            await broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish("ordered", fifo, group_id="g1")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert received == ["ordered"]

    async def test_fifo_request_attempt_id(
        self, queue: str, event: asyncio.Event
    ) -> None:
        broker = self.get_broker(apply_types=True)
        fifo = FifoQueue(name=f"{queue}.fifo", content_based_deduplication=True)
        received: list[str] = []

        # ReceiveRequestAttemptId is accepted by SQS only for FIFO queues; this
        # verifies the kwarg is threaded through and does not break receiving.
        @broker.subscriber(fifo, request_attempt_id="attempt-1")
        async def handler(msg: str) -> None:
            received.append(msg)
            event.set()

        async with broker:
            await broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish("ordered", fifo, group_id="g1")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert received == ["ordered"]
