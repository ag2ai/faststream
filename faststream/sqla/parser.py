from dataclasses import dataclass
from typing import Any

from faststream._internal.basic_types import DecodedMessage
from faststream.message import StreamMessage
from faststream.message.utils import decode_message
from faststream.sqla.message import SqlaInnerMessage, SqlaMessage


@dataclass
class SqlaParser:
    async def parse_message(
        self,
        message: SqlaInnerMessage,
    ) -> "StreamMessage[SqlaInnerMessage]":
        return SqlaMessage(
            raw_message=message,
            body=message.payload,
            headers=message.headers,
            content_type=message.headers.get("content-type"),
            correlation_id=message.headers.get("correlation_id"),
        )

    async def decode_message(
        self,
        msg: Any,
    ) -> "DecodedMessage":
        """Decodes a message."""
        return decode_message(msg)
