from typing import TYPE_CHECKING

from faststream.prometheus import ConsumeAttrs, MetricsSettingsProvider
from faststream.mq.response import MQPublishCommand

if TYPE_CHECKING:
    from faststream.message.message import StreamMessage
    from faststream.mq.message import MQRawMessage


class MQMetricsSettingsProvider(
    MetricsSettingsProvider["MQRawMessage", MQPublishCommand]
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "ibm_mq"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[MQRawMessage]",
    ) -> ConsumeAttrs:
        return {
            "destination_name": msg.raw_message.queue,
            "message_size": len(msg.body),
            "messages_count": 1,
        }

    def get_publish_destination_name_from_cmd(self, cmd: MQPublishCommand) -> str:
        return cmd.destination
