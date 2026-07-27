from typing import TYPE_CHECKING, cast

from faststream.message.message import MsgType
from faststream.prometheus import ConsumeAttrs, MetricsSettingsProvider
from faststream.sqs.message import SQSRawMessage
from faststream.sqs.response import SQSPublishCommand

if TYPE_CHECKING:
    from collections.abc import Sequence

    from faststream.message.message import StreamMessage


class BaseSQSMetricsSettingsProvider(
    MetricsSettingsProvider[MsgType, SQSPublishCommand],
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "aws_sqs"

    def get_publish_destination_name_from_cmd(
        self,
        cmd: SQSPublishCommand,
    ) -> str:
        return cmd.destination


class SQSMetricsSettingsProvider(BaseSQSMetricsSettingsProvider[SQSRawMessage]):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[SQSRawMessage]",
    ) -> ConsumeAttrs:
        return {
            "destination_name": getattr(msg, "queue_url", ""),
            "message_size": len(msg.body),
            "messages_count": 1,
        }


class BatchSQSMetricsSettingsProvider(
    BaseSQSMetricsSettingsProvider[list[SQSRawMessage]],
):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[list[SQSRawMessage]]",
    ) -> ConsumeAttrs:
        # an SQSBatchMessage body is the list of the per-message bodies
        bodies = cast("Sequence[bytes]", msg.body)
        return {
            "destination_name": getattr(msg, "queue_url", ""),
            "message_size": sum(len(body) for body in bodies),
            "messages_count": len(msg.raw_message),
        }


def settings_provider_factory(
    msg: SQSRawMessage | list[SQSRawMessage] | None,
) -> SQSMetricsSettingsProvider | BatchSQSMetricsSettingsProvider:
    if isinstance(msg, list):
        return BatchSQSMetricsSettingsProvider()
    return SQSMetricsSettingsProvider()
