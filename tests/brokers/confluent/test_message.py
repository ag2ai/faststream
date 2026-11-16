import pytest

from faststream.confluent.message import KafkaMessage
from faststream.confluent.schemas import TopicPartition


class RecordingConsumer:
    def __init__(self) -> None:
        self.seeks: list[tuple[str, int, int]] = []
        self.store_calls: list[list[tuple[str, int, int]]] = []
        self.commits = 0
        self.events: list[str] = []

    async def commit(self) -> None:
        self.commits += 1
        self.events.append("commit")

    async def seek(self, topic: str, partition: int, offset: int) -> None:
        self.seeks.append((topic, partition, offset))
        self.events.append("seek")

    async def store_offsets(self, *topic_partitions: TopicPartition) -> None:
        self.store_calls.append([
            (tp.topic, tp.partition, tp.offset) for tp in topic_partitions
        ])
        self.events.append("store")


class Record:
    def __init__(
        self,
        topic: str | None,
        partition: int | None,
        offset: int | None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str | None:
        return self._topic

    def partition(self) -> int | None:
        return self._partition

    def offset(self) -> int | None:
        return self._offset


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_ack_stores_offset_before_commit() -> None:
    consumer = RecordingConsumer()
    message = KafkaMessage(
        raw_message=Record("topic", 2, 7), body=b"", consumer=consumer, is_manual=True
    )

    await message.ack()

    assert consumer.store_calls == [[("topic", 2, 8)]]
    assert consumer.commits == 1
    assert consumer.events == ["store", "commit"]
    assert consumer.seeks == []


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_batch_ack_stores_max_offset_per_partition_then_commits_once() -> None:
    consumer = RecordingConsumer()
    batch = (
        Record("topic", 0, 1),
        Record("topic", 0, 2),
        Record("topic", 0, 0),
        Record("topic", 1, 0),
        Record("topic", 1, 1),
        Record("other", 0, 5),
    )
    message = KafkaMessage(raw_message=batch, body=b"", consumer=consumer, is_manual=True)

    await message.ack()

    assert len(consumer.store_calls) == 1
    assert sorted(consumer.store_calls[0]) == [
        ("other", 0, 6),
        ("topic", 0, 3),
        ("topic", 1, 2),
    ]
    assert consumer.commits == 1
    assert consumer.events == ["store", "commit"]


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_ack_skips_records_without_topic_partition_or_offset() -> None:
    consumer = RecordingConsumer()
    batch = (
        Record(None, 0, 1),
        Record("topic", None, 2),
        Record("topic", 0, None),
        Record("topic", 0, 4),
    )
    message = KafkaMessage(raw_message=batch, body=b"", consumer=consumer, is_manual=True)

    await message.ack()

    assert consumer.store_calls == [[("topic", 0, 5)]]
    assert consumer.commits == 1
    assert consumer.events == ["store", "commit"]


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_ack_without_manual_mode_neither_stores_nor_commits() -> None:
    consumer = RecordingConsumer()
    message = KafkaMessage(
        raw_message=Record("topic", 0, 3), body=b"", consumer=consumer, is_manual=False
    )

    await message.ack()

    assert consumer.events == []
    assert consumer.store_calls == []


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_ack_without_valid_offsets_commits_without_storing() -> None:
    consumer = RecordingConsumer()
    batch = (
        Record(None, 0, 1),
        Record("topic", None, 2),
        Record("topic", 0, None),
    )
    message = KafkaMessage(raw_message=batch, body=b"", consumer=consumer, is_manual=True)

    await message.ack()

    assert consumer.store_calls == []
    assert consumer.commits == 1
    assert consumer.events == ["commit"]


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_batch_nack_rewinds_every_partition_and_never_stores() -> None:
    consumer = RecordingConsumer()
    batch = (
        Record("topic", 0, 1),
        Record("topic", 0, 2),
        Record("topic", 1, 0),
        Record("topic", 1, 1),
        Record("other", 0, 5),
    )
    message = KafkaMessage(raw_message=batch, body=b"", consumer=consumer, is_manual=True)

    await message.nack()

    assert sorted(consumer.seeks) == [("other", 0, 5), ("topic", 0, 1), ("topic", 1, 0)]
    assert consumer.store_calls == []
    assert consumer.commits == 0
    assert "store" not in consumer.events
    assert "commit" not in consumer.events


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_single_message_nack_rewinds_its_partition_and_never_stores() -> None:
    consumer = RecordingConsumer()
    message = KafkaMessage(
        raw_message=Record("topic", 2, 7), body=b"", consumer=consumer, is_manual=True
    )

    await message.nack()

    assert consumer.seeks == [("topic", 2, 7)]
    assert consumer.store_calls == []
    assert consumer.commits == 0
