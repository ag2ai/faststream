from typing import TYPE_CHECKING, List, Sequence, Union, cast

from nats.aio.msg import Msg

from faststream.broker.message import MsgType, StreamMessage
from faststream.prometheus import (
    ConsumeAttrs,
    MetricsSettingsProvider,
)

if TYPE_CHECKING:
    from faststream.types import AnyDict


class BaseNatsMetricsSettingsProvider(MetricsSettingsProvider[MsgType]):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "nats"

    def get_publish_destination_name_from_kwargs(
        self,
        kwargs: "AnyDict",
    ) -> str:
        return cast("str", kwargs["subject"])


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


class BatchNatsMetricsSettingsProvider(BaseNatsMetricsSettingsProvider[List["Msg"]]):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[List[Msg]]",
    ) -> ConsumeAttrs:
        raw_message = msg.raw_message[0]
        return {
            "destination_name": raw_message.subject,
            "message_size": len(msg.body),
            "messages_count": len(msg.raw_message),
        }


def settings_provider_factory(
    msg: Union["Msg", Sequence["Msg"], None],
) -> Union[
    NatsMetricsSettingsProvider,
    BatchNatsMetricsSettingsProvider,
    None,
]:
    if isinstance(msg, Sequence):
        return BatchNatsMetricsSettingsProvider()
    elif isinstance(msg, Msg) or msg is None:
        return NatsMetricsSettingsProvider()
    else:
        # KeyValue and Object Storage watch cases
        return None
