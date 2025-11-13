from typing import Any, Protocol

from confluent_kafka import Message

from faststream.message import AckStatus, StreamMessage


class ConsumerProtocol(Protocol):
    """A protocol for Kafka consumers."""

    async def commit(self) -> None: ...

    async def seek(
        self,
        topic: str,
        partition: int,
        offset: int,
    ) -> None: ...


class FakeConsumer:
    """A fake Kafka consumer."""

    async def commit(self) -> None:
        pass

    async def seek(
        self,
        topic: str | None,
        partition: int | None,
        offset: int | None,
    ) -> None:
        pass


FAKE_CONSUMER = FakeConsumer()


class KafkaMessage(
    StreamMessage[Message | tuple[Message, ...]],
):
    """Represents a Kafka message in the FastStream framework.

    This class extends `StreamMessage` and is specialized for handling confluent_kafka.Message objects.
    """

    def __init__(
        self,
        *args: Any,
        consumer: ConsumerProtocol,
        is_manual: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.consumer = consumer

        self.is_manual = is_manual
        if not is_manual:
            self.committed = AckStatus.ACKED

    async def ack(self) -> None:
        """Acknowledge the Kafka message."""
        if self.is_manual and not self.committed:
            await self.consumer.commit()
        await super().ack()

    async def reject(self) -> None:
        await self.ack()

    async def nack(self) -> None:
        """Reject the Kafka message."""
        if self.is_manual and not self.committed:
            raw_message: Message = (
                self.raw_message[0]
                if isinstance(self.raw_message, tuple)
                else self.raw_message
            )
            await self.consumer.seek(
                topic=raw_message.topic(),
                partition=raw_message.partition(),
                offset=raw_message.offset(),
            )
        await super().nack()
