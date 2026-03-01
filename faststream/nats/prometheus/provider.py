from collections.abc import Sequence
from typing import TYPE_CHECKING, Union

from nats.aio.msg import Msg

from faststream.message.message import MsgType, StreamMessage
from faststream.prometheus import (
    ConsumeAttrs,
    MetricsSettingsProvider,
)

if TYPE_CHECKING:
    from faststream.response import PublishCommand


class BaseNatsMetricsSettingsProvider(MetricsSettingsProvider[MsgType]):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "nats"

    def get_publish_destination_name_from_cmd(
        self,
        cmd: "PublishCommand",
    ) -> str:
        return cmd.destination


class NatsMetricsSettingsProvider(BaseNatsMetricsSettingsProvider["Msg"]):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[Msg]",
    ) -> ConsumeAttrs:
        return {
            "destination_name": msg.raw_message.subject,
            "message_size": len(msg.body),
            "messages_count": 1,
        }


class BatchNatsMetricsSettingsProvider(BaseNatsMetricsSettingsProvider[list["Msg"]]):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[list[Msg]]",
    ) -> ConsumeAttrs:
        raw_message = msg.raw_message[0]
        return {
            "destination_name": raw_message.subject,
            "message_size": len(msg.body),
            "messages_count": len(msg.raw_message),
        }


def settings_provider_factory(
    msg: Union["Msg", Sequence["Msg"], None],
) -> NatsMetricsSettingsProvider | BatchNatsMetricsSettingsProvider | None:
    if isinstance(msg, Sequence):
        return BatchNatsMetricsSettingsProvider()
    if isinstance(msg, Msg) or msg is None:
        return NatsMetricsSettingsProvider()
    # KeyValue and Object Storage watch cases
    return None
