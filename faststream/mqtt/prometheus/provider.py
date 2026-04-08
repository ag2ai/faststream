from typing import TYPE_CHECKING

import zmqtt

from faststream.mqtt.response import MQTTPublishCommand
from faststream.prometheus import ConsumeAttrs, MetricsSettingsProvider

if TYPE_CHECKING:
    from faststream.message.message import StreamMessage


class MQTTMetricsSettingsProvider(
    MetricsSettingsProvider[zmqtt.Message, MQTTPublishCommand],
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "mqtt"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[zmqtt.Message]",
    ) -> ConsumeAttrs:
        return {
            "destination_name": msg.raw_message.topic,
            "message_size": len(msg.body),
            "messages_count": 1,
        }

    def get_publish_destination_name_from_cmd(
        self,
        cmd: MQTTPublishCommand,
    ) -> str:
        return cmd.destination
