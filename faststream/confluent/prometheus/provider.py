from typing import TYPE_CHECKING, Sequence, Tuple, Union, cast

from faststream.broker.message import MsgType, StreamMessage
from faststream.prometheus import (
    ConsumeAttrs,
    MetricsSettingsProvider,
)

if TYPE_CHECKING:
    from confluent_kafka import Message

    from faststream.types import AnyDict


class BaseConfluentMetricsSettingsProvider(MetricsSettingsProvider[MsgType]):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "kafka"

    def get_publish_destination_name_from_kwargs(
        self,
        kwargs: "AnyDict",
    ) -> str:
        return cast("str", kwargs["topic"])


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
    BaseConfluentMetricsSettingsProvider[Tuple["Message", ...]]
):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[Tuple[Message, ...]]",
    ) -> ConsumeAttrs:
        raw_message = msg.raw_message[0]
        return {
            "destination_name": cast("str", raw_message.topic()),
            "message_size": len(bytearray().join(cast("Sequence[bytes]", msg.body))),
            "messages_count": len(msg.raw_message),
        }


def settings_provider_factory(
    msg: Union["Message", Sequence["Message"], None],
) -> Union[
    ConfluentMetricsSettingsProvider,
    BatchConfluentMetricsSettingsProvider,
]:
    if isinstance(msg, Sequence):
        return BatchConfluentMetricsSettingsProvider()
    else:
        return ConfluentMetricsSettingsProvider()
