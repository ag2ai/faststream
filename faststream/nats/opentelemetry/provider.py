from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from nats.aio.msg import Msg
from opentelemetry.semconv.trace import SpanAttributes

from faststream._internal.types import MsgType
from faststream.opentelemetry import TelemetrySettingsProvider
from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME

if TYPE_CHECKING:
    from faststream.message import StreamMessage
    from faststream.response import PublishCommand


class BaseNatsTelemetrySettingsProvider(TelemetrySettingsProvider[MsgType]):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "nats"

    def get_publish_attrs_from_cmd(
        self,
        cmd: "PublishCommand",
    ) -> dict[str, Any]:
        return {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_DESTINATION_NAME: cmd.destination,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: cmd.correlation_id,
        }

    def get_publish_destination_name(
        self,
        cmd: "PublishCommand",
    ) -> str:
        return cmd.destination


class NatsTelemetrySettingsProvider(BaseNatsTelemetrySettingsProvider["Msg"]):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[Msg]",
    ) -> dict[str, Any]:
        return {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_MESSAGE_ID: msg.message_id,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
            MESSAGING_DESTINATION_PUBLISH_NAME: msg.raw_message.subject,
        }

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[Msg]",
    ) -> str:
        return msg.raw_message.subject


class NatsBatchTelemetrySettingsProvider(
    BaseNatsTelemetrySettingsProvider[list["Msg"]],
):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[list[Msg]]",
    ) -> dict[str, Any]:
        return {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_MESSAGE_ID: msg.message_id,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
            SpanAttributes.MESSAGING_BATCH_MESSAGE_COUNT: len(msg.raw_message),
            MESSAGING_DESTINATION_PUBLISH_NAME: msg.raw_message[0].subject,
        }

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[list[Msg]]",
    ) -> str:
        return msg.raw_message[0].subject


@overload
def telemetry_attributes_provider_factory(
    msg: Optional["Msg"],
) -> NatsTelemetrySettingsProvider: ...


@overload
def telemetry_attributes_provider_factory(
    msg: Sequence["Msg"],
) -> NatsBatchTelemetrySettingsProvider: ...


@overload
def telemetry_attributes_provider_factory(
    msg: Union["Msg", Sequence["Msg"], None],
) -> NatsTelemetrySettingsProvider | NatsBatchTelemetrySettingsProvider: ...


def telemetry_attributes_provider_factory(
    msg: Union["Msg", Sequence["Msg"], None],
) -> NatsTelemetrySettingsProvider | NatsBatchTelemetrySettingsProvider | None:
    if isinstance(msg, Sequence):
        return NatsBatchTelemetrySettingsProvider()
    if isinstance(msg, Msg) or msg is None:
        return NatsTelemetrySettingsProvider()
    # KeyValue and Object Storage watch cases
    return None
