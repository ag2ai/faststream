from typing import Any, Protocol, Union

from aiokafka import (
    AIOKafkaConsumer,
    ConsumerRecord,
    TopicPartition as AIOKafkaTopicPartition,
)

from faststream.message import AckStatus, StreamMessage


class ConsumerProtocol(Protocol):
    """A protocol for Kafka consumers."""

    __slots__ = ()

    async def commit(self) -> None: ...

    def seek(
        self,
        partition: AIOKafkaTopicPartition,
        offset: int,
    ) -> None:
        pass


class FakeConsumer:
    """A fake Kafka consumer."""

    async def commit(self) -> None:
        pass

    def seek(
        self,
        partition: AIOKafkaTopicPartition,
        offset: int,
    ) -> None:
        pass


FAKE_CONSUMER = FakeConsumer()


class KafkaRawMessage(ConsumerRecord[Any, Any]):  # type: ignore[misc]
    consumer: AIOKafkaConsumer


class KafkaMessage(
    StreamMessage[
        Union[
            "ConsumerRecord[Any, Any]",
            tuple["ConsumerRecord[Any, Any]", ...],
        ]
    ],
):
    """Represents a Kafka message in the FastStream framework.

    This class extends `StreamMessage` and is specialized for handling Kafka ConsumerRecord objects.
    """

    __slots__ = ("consumer",)

    def __init__(self, *args: Any, consumer: ConsumerProtocol, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.consumer = consumer
        self.committed = AckStatus.ACKED


class KafkaAckableMessage(KafkaMessage):
    __slots__ = ()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.committed = None

    async def ack(self) -> None:
        if not self.committed:
            await self.consumer.commit()
        await super().ack()

    async def reject(self) -> None:
        await self.ack()

    async def nack(self) -> None:
        if not self.committed:
            # A batch may span several partitions: rewind each one to its earliest
            # record, so none of them is committed past an unprocessed message.
            records = (
                self.raw_message
                if isinstance(self.raw_message, tuple)
                else (self.raw_message,)
            )
            earliest: dict[AIOKafkaTopicPartition, int] = {}
            for record in records:
                topic_partition = AIOKafkaTopicPartition(record.topic, record.partition)
                offset = earliest.get(topic_partition)
                if offset is None or record.offset < offset:
                    earliest[topic_partition] = record.offset
            for topic_partition, offset in earliest.items():
                self.consumer.seek(partition=topic_partition, offset=offset)
        await super().nack()
