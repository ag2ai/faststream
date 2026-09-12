import datetime
from typing import TYPE_CHECKING, Optional

from aio_pika import Message
from aio_pika.abc import DeliveryMode

from faststream._internal.parser import DefaultCodec
from faststream._internal.types import IdGenerator
from faststream._internal.utils.path import match_path
from faststream.message import (
    StreamMessage,
    decode_message,
    gen_cor_id,
)
from faststream.rabbit.message import RabbitMessage

if TYPE_CHECKING:
    from re import Pattern

    from aio_pika import IncomingMessage
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import DecodedMessage
    from faststream._internal.parser import CodecProto
    from faststream.rabbit.response import RabbitPublishCommand


class AioPikaParser:
    """A class for parsing, encoding, and decoding messages using aio-pika."""

    def __init__(self, pattern: Optional["Pattern[str]"] = None) -> None:
        self.pattern = pattern

    async def parse_message(
        self,
        message: "IncomingMessage",
    ) -> StreamMessage["IncomingMessage"]:
        """Parses an incoming message and returns a RabbitMessage object."""
        path = match_path(self.pattern, message.routing_key or "")

        return RabbitMessage(
            body=message.body,
            headers=message.headers,
            reply_to=message.reply_to or "",
            content_type=message.content_type,
            message_id=message.message_id or gen_cor_id(),
            correlation_id=message.correlation_id or gen_cor_id(),
            path=path,
            raw_message=message,
        )

    async def decode_message(
        self,
        msg: StreamMessage["IncomingMessage"],
    ) -> "DecodedMessage":
        """Decode a message."""
        return decode_message(msg)

    @staticmethod
    async def encode_message(
        cmd: "RabbitPublishCommand",
        *,
        serializer: Optional["SerializerProto"] = None,
        codec: Optional["CodecProto"] = None,
        id_generator: IdGenerator = gen_cor_id,
    ) -> Message:
        """Encodes a message for sending using AioPika."""
        if isinstance(cmd.body, Message):
            return cmd.body

        encoded = await (codec or DefaultCodec()).encode(cmd, serializer)

        opts = cmd.message_options
        persist = opts.get("persist", False)
        delivery_mode = (
            DeliveryMode.PERSISTENT if persist else DeliveryMode.NOT_PERSISTENT
        )

        return Message(
            encoded.body,
            content_type=opts.get("content_type") or encoded.content_type,
            delivery_mode=delivery_mode,
            reply_to=cmd.reply_to or None,
            correlation_id=cmd.correlation_id or id_generator(),
            headers=cmd.headers,
            content_encoding=opts.get("content_encoding"),
            priority=opts.get("priority"),
            expiration=opts.get("expiration"),
            message_id=opts.get("message_id"),
            timestamp=opts.get("timestamp")
            or datetime.datetime.now(tz=datetime.timezone.utc),
            type=opts.get("message_type"),
            user_id=opts.get("user_id"),
            app_id=opts.get("app_id"),
        )
