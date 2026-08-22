from typing import TYPE_CHECKING, Any

from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_CONVERSATION_ID,
    MESSAGING_MESSAGE_ID,
    MESSAGING_RABBITMQ_DESTINATION_ROUTING_KEY,
    MESSAGING_SYSTEM,
)

from faststream.opentelemetry import TelemetrySettingsProvider
from faststream.opentelemetry.consts import (
    MESSAGING_DESTINATION_PUBLISH_NAME,
    MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES,
)
from faststream.rabbit.response import RabbitPublishCommand

if TYPE_CHECKING:
    from aio_pika import IncomingMessage

    from faststream.message import StreamMessage


class RabbitTelemetrySettingsProvider(
    TelemetrySettingsProvider["IncomingMessage", RabbitPublishCommand],
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "rabbitmq"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[IncomingMessage]",
    ) -> dict[str, Any]:
        return {
            MESSAGING_SYSTEM: self.messaging_system,
            MESSAGING_MESSAGE_ID: msg.message_id,
            MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
            MESSAGING_RABBITMQ_DESTINATION_ROUTING_KEY: msg.raw_message.routing_key,
            "messaging.rabbitmq.message.delivery_tag": msg.raw_message.delivery_tag,
            MESSAGING_DESTINATION_PUBLISH_NAME: msg.raw_message.exchange,
        }

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[IncomingMessage]",
    ) -> str:
        exchange = msg.raw_message.exchange or "default"
        routing_key = msg.raw_message.routing_key
        return f"{exchange}.{routing_key}"

    def get_publish_attrs_from_cmd(
        self,
        cmd: "RabbitPublishCommand",
    ) -> dict[str, Any]:
        return {
            MESSAGING_SYSTEM: self.messaging_system,
            MESSAGING_DESTINATION_NAME: cmd.exchange.name,
            MESSAGING_RABBITMQ_DESTINATION_ROUTING_KEY: cmd.destination,
            MESSAGING_MESSAGE_CONVERSATION_ID: cmd.correlation_id,
        }

    def get_publish_destination_name(
        self,
        cmd: "RabbitPublishCommand",
    ) -> str:
        return f"{cmd.exchange.name or 'default'}.{cmd.destination}"
