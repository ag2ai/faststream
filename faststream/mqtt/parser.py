from contextlib import suppress
from typing import TYPE_CHECKING, Any

import zmqtt

from faststream._internal._compat import json_loads
from faststream.message import StreamMessage, decode_message

from .message import MQTTMessage

if TYPE_CHECKING:
    from faststream._internal.basic_types import DecodedMessage


class MQTTBaseParser:
    """Base parser for MQTT messages — shared parse + decode logic."""

    async def parse_message(self, msg: zmqtt.Message) -> MQTTMessage:
        raise NotImplementedError

    async def decode_message(self, msg: "StreamMessage[Any]") -> "DecodedMessage":
        return decode_message(msg)


class MQTTParserV311(MQTTBaseParser):
    """Parser for MQTT 3.1.1 messages — raw payload, no metadata."""

    async def parse_message(self, msg: zmqtt.Message) -> MQTTMessage:
        return MQTTMessage(
            raw_message=msg,
            body=msg.payload,
            headers={},
            content_type=None,
            reply_to="",
            correlation_id=None,
        )

    async def decode_message(self, msg: "StreamMessage[Any]") -> "DecodedMessage":
        body: bytes = msg.body
        with suppress(Exception):
            m: DecodedMessage = json_loads(body)
            return m
        with suppress(UnicodeDecodeError):
            return body.decode()
        return body


class MQTTParserV5(MQTTBaseParser):
    """Parser for MQTT 5.0 messages.

    Extracts content_type, response_topic, correlation_data, and
    user_properties from PUBLISH properties when available.
    """

    async def parse_message(self, msg: zmqtt.Message) -> MQTTMessage:
        props = msg.properties
        content_type: str | None = None
        reply_to: str = ""
        correlation_id: str | None = None
        headers: dict[str, Any] = {}

        if props is not None:
            content_type = props.content_type
            reply_to = props.response_topic or ""
            if props.correlation_data is not None:
                correlation_id = props.correlation_data.decode(errors="replace")
            headers.update(props.user_properties)

        return MQTTMessage(
            raw_message=msg,
            body=msg.payload,
            headers=headers,
            content_type=content_type,
            reply_to=reply_to,
            correlation_id=correlation_id,
        )
