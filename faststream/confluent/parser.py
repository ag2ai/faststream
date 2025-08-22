from typing import TYPE_CHECKING, Any

from faststream.message import StreamMessage, decode_message

from .message import FAKE_CONSUMER, KafkaMessage

if TYPE_CHECKING:
    from collections.abc import Sequence

    from confluent_kafka import Message

    from faststream._internal.basic_types import DecodedMessage

    from .message import ConsumerProtocol

    # Type of headers returned by confluent_kafka Message.headers()
    _HeadersInput = (
        dict[str, str | bytes | None]
        | list[tuple[str, str | bytes | None]]
        | tuple[tuple[str, str | bytes | None], ...]
    )


class AsyncConfluentParser:
    """A class to parse Kafka messages."""

    def __init__(self, is_manual: bool = False) -> None:
        self.is_manual = is_manual
        self._consumer: ConsumerProtocol = FAKE_CONSUMER

    def _setup(self, consumer: "ConsumerProtocol") -> None:
        self._consumer = consumer

    async def parse_message(
        self,
        message: "Message",
    ) -> KafkaMessage:
        """Parses a Kafka message."""
        headers = dict(message.headers() or ())

        body = message.value() or b""
        offset = message.offset()
        _, timestamp = message.timestamp()

        return KafkaMessage(
            body=body,
            headers=headers,
            reply_to=headers.get("reply_to").decode()
            if "content-type" in headers
            else None,
            content_type=headers.get("content-type").decode()
            if "content-type" in headers
            else None,
            message_id=f"{offset}-{timestamp}",
            correlation_id=headers.get("correlation_id").decode()
            if "correlation_id" in headers
            else None,
            raw_message=message,
            consumer=self._consumer,
            is_manual=self.is_manual,
        )

    async def parse_batch(
        self,
        message: tuple["Message", ...],
    ) -> KafkaMessage:
        """Parses a batch of messages from a Kafka consumer."""
        body: list[Any] = []
        batch_headers: list[dict[str, bytes]] = []

        first = message[0]
        last = message[-1]

        for m in message:
            body.append(m.value() or b"")
            batch_headers.append(dict(m.headers() or ()))

        headers = next(iter(batch_headers), {})

        _, first_timestamp = first.timestamp()

        return KafkaMessage(
            body=body,
            headers=headers,
            batch_headers=batch_headers,
            reply_to=headers.get("reply_to").decode()
            if "content-type" in headers
            else None,
            content_type=headers.get("content-type").decode()
            if "content-type" in headers
            else None,
            message_id=f"{first.offset()}-{last.offset()}-{first_timestamp}",
            correlation_id=headers.get("correlation_id").decode()
            if "correlation_id" in headers
            else None,
            raw_message=message,
            consumer=self._consumer,
            is_manual=self.is_manual,
        )

    async def decode_message(
        self,
        msg: "StreamMessage[Message]",
    ) -> "DecodedMessage":
        """Decodes a message."""
        return decode_message(msg)

    async def decode_batch(
        self,
        msg: "StreamMessage[tuple[Message, ...]]",
    ) -> "DecodedMessage":
        """Decode a batch of messages."""
        return [decode_message(await self.parse_message(m)) for m in msg.raw_message]
