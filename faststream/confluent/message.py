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

    @property
    def raw_headers(self) -> dict[str, bytes | None]:
        """Original header bytes, or the first message's headers for a batch."""
        raw_message = self.raw_message
        if isinstance(raw_message, tuple):
            if not raw_message:
                return {}
            raw_message = raw_message[0]
        return _get_raw_headers(raw_message)

    @property
    def batch_raw_headers(self) -> list[dict[str, bytes | None]]:
        """Original header bytes for every batch message; empty for a single message."""
        if isinstance(self.raw_message, tuple):
            return [_get_raw_headers(message) for message in self.raw_message]
        return []

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
            # A batch may span several partitions: rewind each one to its earliest
            # record, so none of them is committed past an unprocessed message.
            records: tuple[Message, ...] = (
                self.raw_message
                if isinstance(self.raw_message, tuple)
                else (self.raw_message,)
            )
            earliest: dict[tuple[str, int], int] = {}
            for record in records:
                topic = record.topic()
                partition = record.partition()
                offset = record.offset()
                if topic is None or partition is None or offset is None:
                    continue
                known = earliest.get((topic, partition))
                if known is None or offset < known:
                    earliest[topic, partition] = offset
            for (topic, partition), offset in earliest.items():
                await self.consumer.seek(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                )
        await super().nack()


def _get_raw_headers(message: Message) -> dict[str, bytes | None]:
    headers = dict(message.headers() or ())
    return {
        key: value.encode() if isinstance(value, str) else value
        for key, value in headers.items()
    }
