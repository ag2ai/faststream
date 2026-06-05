from typing import TYPE_CHECKING

from faststream.prometheus import ConsumeAttrs, MetricsSettingsProvider
from faststream.sqs.message import SQSRawMessage
from faststream.sqs.response import SQSPublishCommand

if TYPE_CHECKING:
    from faststream.message.message import StreamMessage


class SQSMetricsSettingsProvider(
    MetricsSettingsProvider[SQSRawMessage, SQSPublishCommand],
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "aws_sqs"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[SQSRawMessage]",
    ) -> ConsumeAttrs:
        return {
            "destination_name": getattr(msg, "queue_url", ""),
            "message_size": len(msg.body),
            "messages_count": 1,
        }

    def get_publish_destination_name_from_cmd(
        self,
        cmd: SQSPublishCommand,
    ) -> str:
        return cmd.destination
