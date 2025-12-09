from typing import TYPE_CHECKING, cast

from faststream._internal.basic_types import DecodedMessage
from faststream._internal.constants import ContentTypes
from faststream.message import decode_message
from faststream.mqtt.message import MQTTMessage

if TYPE_CHECKING:
    from aiomqtt import Message

    from faststream._internal.basic_types import DecodedMessage
    from faststream.message import StreamMessage


class MQTTParser:
    async def parse_message(self, message: "Message") -> MQTTMessage:
        payload = message.payload
        ct = (
            message.properties.ContentType  # type: ignore[attr-defined]
            if message.properties
            else ContentTypes.TEXT.value
        )
        return MQTTMessage(
            raw_message=message,
            body=payload,
            topic=cast("str", message.topic),
            content_type=ct,
        )

    async def decode_message(
        self,
        msg: "StreamMessage[Message]",
    ) -> "DecodedMessage":
        return decode_message(msg)
