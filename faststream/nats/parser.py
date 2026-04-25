from typing import TYPE_CHECKING, Any

from faststream._internal.utils.path import match_path
from faststream.message import (
    StreamMessage,
    decode_message,
)
from faststream.nats.message import (
    NatsBatchMessage,
    NatsKvMessage,
    NatsMessage,
    NatsObjMessage,
)
from faststream.nats.schemas.js_stream import compile_nats_wildcard

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.js.api import ObjectInfo
    from nats.js.kv import KeyValue

    from faststream._internal.basic_types import DecodedMessage


class NatsBaseParser:
    """A class to parse NATS messages."""

    def __init__(
        self,
        *,
        pattern: str,
    ) -> None:
        path_re, _ = compile_nats_wildcard(pattern)
        self._path_re = path_re

    async def decode_message(
        self,
        msg: "StreamMessage[Any]",
    ) -> "DecodedMessage":
        return decode_message(msg)


class NatsParser(NatsBaseParser):
    """A class to parse NATS core messages."""

    def __init__(self, *, pattern: str, is_ack_disabled: bool) -> None:
        super().__init__(pattern=pattern)

        self.is_ack_disabled = is_ack_disabled

    async def parse_message(
        self,
        message: "Msg",
    ) -> "StreamMessage[Msg]":
        path = match_path(self._path_re, message.subject)

        headers = message.header or {}

        if self.is_ack_disabled:
            message._ackd = True

        return NatsMessage(
            raw_message=message,
            body=message.data,
            path=path,
            reply_to=message.reply,
            headers=headers,
            content_type=headers.get("content-type", ""),
            message_id=headers.get("message_id"),
            correlation_id=headers.get("correlation_id"),
        )


class JsParser(NatsBaseParser):
    """A class to parse NATS JS messages."""

    async def parse_message(
        self,
        message: "Msg",
    ) -> "StreamMessage[Msg]":
        path = match_path(self._path_re, message.subject)

        headers = message.header or {}

        return NatsMessage(
            raw_message=message,
            body=message.data,
            path=path,
            reply_to=headers.get("reply_to", ""),  # differ from core
            headers=headers,
            content_type=headers.get("content-type"),
            message_id=headers.get("message_id"),
            correlation_id=headers.get("correlation_id"),
        )


class BatchParser(JsParser):
    """A class to parse NATS batch messages."""

    async def parse_batch(
        self,
        message: list["Msg"],
    ) -> "StreamMessage[list[Msg]]":
        body: list[bytes] = []
        batch_headers: list[dict[str, str]] = []

        if message:
            path = match_path(self._path_re, message[0].subject)

            for m in message:
                batch_headers.append(m.headers or {})
                body.append(m.data)

        else:
            path = {}

        headers = next(iter(batch_headers), {})

        return NatsBatchMessage(
            raw_message=message,
            body=body,
            path=path,
            headers=headers,
            batch_headers=batch_headers,
        )

    async def decode_batch(
        self,
        msg: "StreamMessage[list[Msg]]",
    ) -> list["DecodedMessage"]:
        data: list[DecodedMessage] = []

        for m in msg.raw_message:
            one_msg = await self.parse_message(m)
            data.append(decode_message(one_msg))

        return data


class KvParser(NatsBaseParser):
    async def parse_message(
        self,
        msg: "KeyValue.Entry",
    ) -> StreamMessage["KeyValue.Entry"]:
        return NatsKvMessage(
            raw_message=msg,
            body=msg.value,
            path=match_path(self._path_re, msg.key),
        )


class ObjParser(NatsBaseParser):
    async def parse_message(self, msg: "ObjectInfo") -> StreamMessage["ObjectInfo"]:
        return NatsObjMessage(
            raw_message=msg,
            body=msg.name,
        )
