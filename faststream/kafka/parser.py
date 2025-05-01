from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, Union, cast

from faststream.broker.message import decode_message, gen_cor_id
from faststream.kafka.message import FAKE_CONSUMER, KafkaMessage, KafkaRawMessage
from faststream.utils.context.repository import context

if TYPE_CHECKING:
    from re import Pattern

    from aiokafka import ConsumerRecord

    from faststream.broker.message import StreamMessage
    from faststream.kafka.subscriber.usecase import LogicSubscriber
    from faststream.types import DecodedMessage


class AioKafkaParser:
    """A class to parse Kafka messages."""

    def __init__(
        self,
        msg_class: Type[KafkaMessage],
        regex: Optional["Pattern[str]"],
    ) -> None:
        self.msg_class = msg_class
        self.regex = regex

    async def parse_message(
        self,
        message: Union["ConsumerRecord", "KafkaRawMessage"],
    ) -> "StreamMessage[ConsumerRecord]":
        """Parses a Kafka message."""
        headers = {i: j.decode() for i, j in message.headers}
        handler: Optional[LogicSubscriber[Any]] = context.get_local("handler_")

        return self.msg_class(
            body=message.value or b"",
            headers=headers,
            reply_to=headers.get("reply_to", ""),
            content_type=headers.get("content-type"),
            message_id=f"{message.offset}-{message.timestamp}",
            correlation_id=headers.get("correlation_id", gen_cor_id()),
            raw_message=message,
            path=self.get_path(message.topic),
            consumer=getattr(message, "consumer", None)
            or getattr(handler, "consumer", None)
            or FAKE_CONSUMER,
        )

    async def decode_message(
        self,
        msg: "StreamMessage[ConsumerRecord]",
    ) -> "DecodedMessage":
        """Decodes a message."""
        return decode_message(msg)

    def get_path(self, topic: str) -> Dict[str, str]:
        if self.regex and (match := self.regex.match(topic)):
            return match.groupdict()
        else:
            return {}


class AioKafkaBatchParser(AioKafkaParser):
    async def parse_message(
        self,
        message: Tuple["ConsumerRecord", ...],
    ) -> "StreamMessage[Tuple[ConsumerRecord, ...]]":
        """Parses a batch of messages from a Kafka consumer."""
        body: List[Any] = []
        batch_headers: List[Dict[str, str]] = []

        first = message[0]
        last = message[-1]

        for m in message:
            body.append(m.value or b"")
            batch_headers.append({i: j.decode() for i, j in m.headers})

        headers = next(iter(batch_headers), {})

        handler: Optional[LogicSubscriber[Any]] = context.get_local("handler_")

        return self.msg_class(
            body=body,
            headers=headers,
            batch_headers=batch_headers,
            reply_to=headers.get("reply_to", ""),
            content_type=headers.get("content-type"),
            message_id=f"{first.offset}-{last.offset}-{first.timestamp}",
            correlation_id=headers.get("correlation_id", gen_cor_id()),
            raw_message=message,
            path=self.get_path(first.topic),
            consumer=getattr(handler, "consumer", None) or FAKE_CONSUMER,
        )

    async def decode_message(
        self,
        msg: "StreamMessage[Tuple[ConsumerRecord, ...]]",
    ) -> "DecodedMessage":
        """Decode a batch of messages."""
        # super() should be here due python can't find it in comprehension
        super_obj = cast("AioKafkaParser", super())

        return [
            decode_message(await super_obj.parse_message(m)) for m in msg.raw_message
        ]
