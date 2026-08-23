from contextlib import suppress
from typing import TYPE_CHECKING, Any

import zmqtt

from faststream._internal._compat import json_loads
from faststream._internal.utils.path import match_path
from faststream.message import StreamMessage, decode_message

from .message import MQTTMessage

if TYPE_CHECKING:
    from re import Pattern

    from faststream._internal.basic_types import DecodedMessage


class MQTTBaseParser:
    """Base parser for MQTT messages — shared parse + decode logic."""

    def __init__(
        self,
        regex: "Pattern[str] | None" = None,
    ) -> None:
        self.regex = regex
        """Captures each Path parameter out of an incoming topic.

        A value rather than a way to ask for one: the parser is built during
        Preparation, when the topic it compiles from is resolved.
        """

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
            path=match_path(self.regex, msg.topic),
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
            path=match_path(self.regex, msg.topic),
            content_type=content_type,
            reply_to=reply_to,
            correlation_id=correlation_id,
        )


def parser_for(version: str) -> type[MQTTBaseParser]:
    """The parser class a Broker version speaks.

    One place says it, because both the Subscriber that consumes through a
    parser and the in-memory producer that encodes for one have to agree on
    which version they are speaking.
    """
    return MQTTParserV311 if version == "3.1.1" else MQTTParserV5
