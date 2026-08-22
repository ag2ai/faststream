from typing import TYPE_CHECKING, Any

import zmqtt
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_CONVERSATION_ID,
    MESSAGING_MESSAGE_ID,
    MESSAGING_SYSTEM,
)

from faststream.mqtt.response import MQTTPublishCommand
from faststream.opentelemetry import TelemetrySettingsProvider
from faststream.opentelemetry.consts import (
    MESSAGING_DESTINATION_PUBLISH_NAME,
    MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES,
)

if TYPE_CHECKING:
    from faststream.message import StreamMessage


class MQTTTelemetrySettingsProvider(
    TelemetrySettingsProvider[zmqtt.Message, MQTTPublishCommand],
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "mqtt"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[zmqtt.Message]",
    ) -> dict[str, Any]:
        return {
            MESSAGING_SYSTEM: self.messaging_system,
            MESSAGING_MESSAGE_ID: msg.message_id,
            MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
            MESSAGING_DESTINATION_PUBLISH_NAME: msg.raw_message.topic,
        }

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[zmqtt.Message]",
    ) -> str:
        return msg.raw_message.topic

    def get_publish_attrs_from_cmd(
        self,
        cmd: MQTTPublishCommand,
    ) -> dict[str, Any]:
        return {
            MESSAGING_SYSTEM: self.messaging_system,
            MESSAGING_DESTINATION_NAME: cmd.destination,
            MESSAGING_MESSAGE_CONVERSATION_ID: cmd.correlation_id,
        }

    def get_publish_destination_name(
        self,
        cmd: MQTTPublishCommand,
    ) -> str:
        return cmd.destination
