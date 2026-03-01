from collections.abc import Sequence
from typing import TYPE_CHECKING, Union, cast

from faststream.message.message import MsgType, StreamMessage
from faststream.prometheus import MetricsSettingsProvider

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord

    from faststream.prometheus import ConsumeAttrs
    from faststream.response import PublishCommand


class BaseKafkaMetricsSettingsProvider(MetricsSettingsProvider[MsgType]):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "kafka"

    def get_publish_destination_name_from_cmd(
        self,
        cmd: "PublishCommand",
    ) -> str:
        return cmd.destination


class KafkaMetricsSettingsProvider(BaseKafkaMetricsSettingsProvider["ConsumerRecord"]):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[ConsumerRecord]",
    ) -> "ConsumeAttrs":
        return {
            "destination_name": msg.raw_message.topic,
            "message_size": len(msg.body),
            "messages_count": 1,
        }


class BatchKafkaMetricsSettingsProvider(
    BaseKafkaMetricsSettingsProvider[tuple["ConsumerRecord", ...]],
):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[tuple[ConsumerRecord, ...]]",
    ) -> "ConsumeAttrs":
        raw_message = msg.raw_message[0]
        return {
            "destination_name": raw_message.topic,
            "message_size": len(bytearray().join(cast("Sequence[bytes]", msg.body))),
            "messages_count": len(msg.raw_message),
        }


def settings_provider_factory(
    msg: Union["ConsumerRecord", Sequence["ConsumerRecord"], None],
) -> KafkaMetricsSettingsProvider | BatchKafkaMetricsSettingsProvider:
    if isinstance(msg, Sequence):
        return BatchKafkaMetricsSettingsProvider()
    return KafkaMetricsSettingsProvider()
