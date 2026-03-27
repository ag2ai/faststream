from typing import TYPE_CHECKING, Any

from opentelemetry.semconv.trace import SpanAttributes

from faststream.mq.response import MQPublishCommand
from faststream.opentelemetry import TelemetrySettingsProvider

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
        return {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_DESTINATION_NAME: msg.raw_message.queue,
            SpanAttributes.MESSAGING_MESSAGE_ID: msg.message_id,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
        }

    def get_consume_destination_name(self, msg: "StreamMessage[MQRawMessage]") -> str:
        return msg.raw_message.queue

    def get_publish_attrs_from_cmd(self, cmd: MQPublishCommand) -> dict[str, Any]:
        return {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_DESTINATION_NAME: cmd.destination,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: cmd.correlation_id,
        }

    def get_publish_destination_name(self, cmd: MQPublishCommand) -> str:
        return cmd.destination
