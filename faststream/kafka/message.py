from typing import Any, Protocol, Union

from aiokafka import (
    AIOKafkaConsumer,
    ConsumerRecord,
    TopicPartition as AIOKafkaTopicPartition,
)

from faststream.message import AckStatus, StreamMessage


class ConsumerProtocol(Protocol):
    """A protocol for Kafka consumers."""

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

    def __init__(self, *args: Any, consumer: ConsumerProtocol, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.consumer = consumer
        self.committed = AckStatus.ACKED

    @property
    def raw_headers(self) -> dict[str, bytes | None]:
        """Original header bytes, or the first message's headers for a batch."""
        raw_message = self.raw_message
        if isinstance(raw_message, tuple):
            if not raw_message:
                return {}
            raw_message = raw_message[0]
        return dict(raw_message.headers)

    @property
    def batch_raw_headers(self) -> list[dict[str, bytes | None]]:
        """Original header bytes for every batch message; empty for a single message."""
        if isinstance(self.raw_message, tuple):
            return [dict(message.headers) for message in self.raw_message]
        return []


class KafkaAckableMessage(KafkaMessage):
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
            raw_message = (
                self.raw_message[0]
                if isinstance(self.raw_message, tuple)
                else self.raw_message
            )
            topic_partition = AIOKafkaTopicPartition(
                raw_message.topic,
                raw_message.partition,
            )
            self.consumer.seek(
                partition=topic_partition,
                offset=raw_message.offset,
            )
        await super().nack()
