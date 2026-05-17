from typing import TYPE_CHECKING

from faststream.message import StreamMessage, decode_message
from faststream.mq.message import MQMessage, MQRawMessage

if TYPE_CHECKING:
    from faststream._internal.basic_types import DecodedMessage


class MQParser:
    async def parse_message(
        self,
        message: MQRawMessage,
    ) -> StreamMessage[MQRawMessage]:
        return MQMessage(
            raw_message=message,
            body=message.body,
            headers=message.headers,
            reply_to=message.reply_to,
            content_type=message.content_type,
            correlation_id=message.correlation_id,
            message_id=message.message_id,
        )

    async def decode_message(
        self,
        msg: StreamMessage[MQRawMessage],
    ) -> "DecodedMessage":
        return decode_message(msg)
