from collections.abc import Sequence
from typing import TYPE_CHECKING, Union, cast

from faststream.message.message import MsgType, StreamMessage
from faststream.prometheus import (
    ConsumeAttrs,
    MetricsSettingsProvider,
)

if TYPE_CHECKING:
    from confluent_kafka import Message

    from faststream.response import PublishCommand


class BaseConfluentMetricsSettingsProvider(MetricsSettingsProvider[MsgType]):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "kafka"

    def get_publish_destination_name_from_cmd(
        self,
        cmd: "PublishCommand",
    ) -> str:
        return cmd.destination


class ConfluentMetricsSettingsProvider(BaseConfluentMetricsSettingsProvider["Message"]):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[Message]",
    ) -> ConsumeAttrs:
        return {
            "destination_name": cast("str", msg.raw_message.topic()),
            "message_size": len(msg.body),
            "messages_count": 1,
        }


class BatchConfluentMetricsSettingsProvider(
    BaseConfluentMetricsSettingsProvider[tuple["Message", ...]],
):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[tuple[Message, ...]]",
    ) -> ConsumeAttrs:
        raw_message = msg.raw_message[0]
        return {
            "destination_name": cast("str", raw_message.topic()),
            "message_size": len(bytearray().join(cast("Sequence[bytes]", msg.body))),
            "messages_count": len(msg.raw_message),
        }


def settings_provider_factory(
    msg: Union["Message", Sequence["Message"], None],
) -> ConfluentMetricsSettingsProvider | BatchConfluentMetricsSettingsProvider:
    if isinstance(msg, Sequence):
        return BatchConfluentMetricsSettingsProvider()
    return ConfluentMetricsSettingsProvider()
