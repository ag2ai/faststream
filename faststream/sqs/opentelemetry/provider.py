from typing import TYPE_CHECKING, Any

from opentelemetry.semconv.trace import SpanAttributes

from faststream.opentelemetry import TelemetrySettingsProvider
from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME
from faststream.sqs.message import SQSRawMessage
from faststream.sqs.response import SQSPublishCommand

if TYPE_CHECKING:
    from faststream.message import StreamMessage


class SQSTelemetrySettingsProvider(
    TelemetrySettingsProvider[SQSRawMessage, SQSPublishCommand],
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "aws_sqs"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[SQSRawMessage]",
    ) -> dict[str, Any]:
        return {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_MESSAGE_ID: msg.message_id,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
            MESSAGING_DESTINATION_PUBLISH_NAME: getattr(msg, "queue_url", ""),
        }

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[SQSRawMessage]",
    ) -> str:
        return getattr(msg, "queue_url", "")

    def get_publish_attrs_from_cmd(
        self,
        cmd: SQSPublishCommand,
    ) -> dict[str, Any]:
        return {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_DESTINATION_NAME: cmd.destination,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: cmd.correlation_id,
        }

    def get_publish_destination_name(
        self,
        cmd: SQSPublishCommand,
    ) -> str:
        return cmd.destination
