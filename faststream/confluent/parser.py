from typing import TYPE_CHECKING, Any

from faststream.message import StreamMessage, decode_message

from .message import FAKE_CONSUMER, KafkaMessage

if TYPE_CHECKING:
    from confluent_kafka import Message

    from faststream._internal.basic_types import DecodedMessage

    from .message import ConsumerProtocol


class AsyncConfluentParser:
    """A class to parse Kafka messages."""

    def __init__(self, is_manual: bool = False) -> None:
        self.is_manual = is_manual
        self._consumer: ConsumerProtocol = FAKE_CONSUMER

    def _setup(self, consumer: "ConsumerProtocol") -> None:
        self._consumer = consumer

    @staticmethod
    def _decode_header(headers: dict[str, str | bytes | None], key: str) -> str | None:
        """Decode a single header value safely, handling non-UTF-8 bytes."""
        val = headers.get(key)
        if not val:
            return None
        return val.decode(errors="replace") if isinstance(val, bytes) else val

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
            reply_to=self._decode_header(headers, "reply_to") or "",
            content_type=self._decode_header(headers, "content-type"),
            message_id=f"{offset}-{timestamp}",
            correlation_id=self._decode_header(headers, "correlation_id"),
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
        batch_headers: list[dict[str, str | bytes | None]] = []

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
            reply_to=self._decode_header(headers, "reply_to") or "",
            content_type=self._decode_header(headers, "content-type"),
            message_id=f"{first.offset()}-{last.offset()}-{first_timestamp}",
            correlation_id=self._decode_header(headers, "correlation_id"),
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
