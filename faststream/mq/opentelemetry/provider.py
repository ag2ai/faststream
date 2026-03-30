from typing import TYPE_CHECKING, Any

from opentelemetry.semconv.trace import SpanAttributes

from faststream.mq.response import MQPublishCommand
from faststream.opentelemetry import TelemetrySettingsProvider
from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME

if TYPE_CHECKING:
    from faststream.message import StreamMessage
    from faststream.mq.message import MQRawMessage


class MQTelemetrySettingsProvider(
    TelemetrySettingsProvider["MQRawMessage", MQPublishCommand],
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "ibm_mq"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[MQRawMessage]",
    ) -> dict[str, Any]:
        conversation_id = msg.correlation_id or msg.message_id
        return {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_DESTINATION_NAME: msg.raw_message.queue,
            MESSAGING_DESTINATION_PUBLISH_NAME: msg.raw_message.queue,
            SpanAttributes.MESSAGING_MESSAGE_ID: msg.message_id,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: conversation_id,
            SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
        }

    def get_consume_destination_name(self, msg: "StreamMessage[MQRawMessage]") -> str:
        return msg.raw_message.queue

    def get_publish_attrs_from_cmd(self, cmd: MQPublishCommand) -> dict[str, Any]:
        conversation_id = cmd.correlation_id or cmd.message_id
        attrs = {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_DESTINATION_NAME: cmd.destination,
        }
        if conversation_id is not None:
            attrs[SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID] = conversation_id
        return attrs

    def get_publish_destination_name(self, cmd: MQPublishCommand) -> str:
        return cmd.destination
