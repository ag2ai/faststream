from typing import Any

import pytest
from aiokafka import ConsumerRecord, TopicPartition

from faststream.kafka.message import KafkaAckableMessage


class RecordingConsumer:
    def __init__(self) -> None:
        self.seeks: list[tuple[TopicPartition, int]] = []

    async def commit(self) -> None:
        pass

    def seek(self, partition: TopicPartition, offset: int) -> None:
        self.seeks.append((partition, offset))


def record(topic: str, partition: int, offset: int) -> "ConsumerRecord[Any, Any]":
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=b"",
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=0,
        headers=(),
    )


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_batch_nack_rewinds_every_partition() -> None:
    consumer = RecordingConsumer()
    batch = (
        record("topic", 0, 1),
        record("topic", 0, 2),
        record("topic", 1, 0),
        record("topic", 1, 1),
        record("other", 0, 5),
    )
    message = KafkaAckableMessage(raw_message=batch, body=b"", consumer=consumer)

    await message.nack()

    assert sorted(consumer.seeks) == [
        (TopicPartition("other", 0), 5),
        (TopicPartition("topic", 0), 1),
        (TopicPartition("topic", 1), 0),
    ]


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_single_message_nack_rewinds_its_partition() -> None:
    consumer = RecordingConsumer()
    message = KafkaAckableMessage(
        raw_message=record("topic", 2, 7), body=b"", consumer=consumer
    )

    await message.nack()

    assert consumer.seeks == [(TopicPartition("topic", 2), 7)]
