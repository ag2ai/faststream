from typing import TYPE_CHECKING

from faststream.prometheus import (
    ConsumeAttrs,
    MetricsSettingsProvider,
)
from faststream.rabbit.response import RabbitPublishCommand

if TYPE_CHECKING:
    from aio_pika import IncomingMessage

    from faststream.message.message import StreamMessage


class RabbitMetricsSettingsProvider(
    MetricsSettingsProvider["IncomingMessage", RabbitPublishCommand],
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "rabbitmq"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[IncomingMessage]",
    ) -> ConsumeAttrs:
        exchange = msg.raw_message.exchange or "default"
        routing_key = msg.raw_message.routing_key

        return {
            "destination_name": f"{exchange}.{routing_key}",
            "message_size": len(msg.body),
            "messages_count": 1,
        }

    def get_publish_destination_name_from_cmd(
        self,
        cmd: RabbitPublishCommand,
    ) -> str:
        return f"{cmd.exchange.name or 'default'}.{cmd.destination}"
