import pytest

from faststream.confluent.message import KafkaMessage


class RecordingConsumer:
    def __init__(self) -> None:
        self.seeks: list[tuple[str, int, int]] = []

    async def commit(self) -> None:
        pass

    async def seek(self, topic: str, partition: int, offset: int) -> None:
        self.seeks.append((topic, partition, offset))


class Record:
    def __init__(self, topic: str, partition: int, offset: int) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_batch_nack_rewinds_every_partition() -> None:
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


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_single_message_nack_rewinds_its_partition() -> None:
    consumer = RecordingConsumer()
    message = KafkaMessage(
        raw_message=Record("topic", 2, 7), body=b"", consumer=consumer, is_manual=True
    )

    await message.nack()

    assert consumer.seeks == [("topic", 2, 7)]
