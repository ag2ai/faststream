from typing import TYPE_CHECKING, Any, cast

from opentelemetry.semconv.trace import SpanAttributes

from faststream.opentelemetry import TelemetrySettingsProvider
from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME

if TYPE_CHECKING:
    from faststream.message import StreamMessage
    from faststream.response import PublishCommand


class RedisTelemetrySettingsProvider(TelemetrySettingsProvider[dict[str, Any]]):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "redis"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[dict[str, Any]]",
    ) -> dict[str, Any]:
        attrs = {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_MESSAGE_ID: msg.message_id,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
            MESSAGING_DESTINATION_PUBLISH_NAME: msg.raw_message["channel"],
        }

        if cast("str", msg.raw_message.get("type", "")).startswith("b"):
            attrs[SpanAttributes.MESSAGING_BATCH_MESSAGE_COUNT] = len(
                msg.raw_message["data"],
            )

        return attrs

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[dict[str, Any]]",
    ) -> str:
        return self._get_destination(msg.raw_message)

    def get_publish_attrs_from_cmd(
        self,
        cmd: "PublishCommand",
    ) -> dict[str, Any]:
        return {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_DESTINATION_NAME: cmd.destination,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: cmd.correlation_id,
        }

    def get_publish_destination_name(
        self,
        cmd: "PublishCommand",
    ) -> str:
        return cmd.destination

    @staticmethod
    def _get_destination(kwargs: dict[str, Any]) -> str:
        return kwargs.get("channel") or kwargs.get("list") or kwargs.get("stream") or ""
