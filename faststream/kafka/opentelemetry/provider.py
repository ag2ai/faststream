from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Union, cast

from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_BATCH_MESSAGE_COUNT,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_KAFKA_DESTINATION_PARTITION,
    MESSAGING_KAFKA_MESSAGE_KEY,
    MESSAGING_KAFKA_MESSAGE_OFFSET,
    MESSAGING_MESSAGE_CONVERSATION_ID,
    MESSAGING_MESSAGE_ID,
    MESSAGING_SYSTEM,
)

from faststream._internal.types import MsgType
from faststream.kafka.response import KafkaPublishCommand
from faststream.opentelemetry import TelemetrySettingsProvider
from faststream.opentelemetry.consts import (
    MESSAGING_DESTINATION_PUBLISH_NAME,
    MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES,
)

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord

    from faststream.message import StreamMessage
    from faststream.response import PublishCommand


class BaseKafkaTelemetrySettingsProvider(
    TelemetrySettingsProvider[MsgType, KafkaPublishCommand],
):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "kafka"

    def get_publish_attrs_from_cmd(
        self,
        cmd: "KafkaPublishCommand",
    ) -> dict[str, Any]:
        attrs: dict[str, Any] = {
            MESSAGING_SYSTEM: self.messaging_system,
            MESSAGING_DESTINATION_NAME: cmd.destination,
            MESSAGING_MESSAGE_CONVERSATION_ID: cmd.correlation_id,
        }

        if cmd.partition is not None:
            attrs[MESSAGING_KAFKA_DESTINATION_PARTITION] = cmd.partition

        if cmd.key is not None:
            attrs[MESSAGING_KAFKA_MESSAGE_KEY] = cmd.key

        return attrs

    def get_publish_destination_name(
        self,
        cmd: "PublishCommand",
    ) -> str:
        return cmd.destination


class KafkaTelemetrySettingsProvider(
    BaseKafkaTelemetrySettingsProvider["ConsumerRecord"],
):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[ConsumerRecord]",
    ) -> dict[str, Any]:
        attrs = {
            MESSAGING_SYSTEM: self.messaging_system,
            MESSAGING_MESSAGE_ID: msg.message_id,
            MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
            MESSAGING_KAFKA_DESTINATION_PARTITION: msg.raw_message.partition,
            MESSAGING_KAFKA_MESSAGE_OFFSET: msg.raw_message.offset,
            MESSAGING_DESTINATION_PUBLISH_NAME: msg.raw_message.topic,
        }

        if msg.raw_message.key is not None:
            attrs[MESSAGING_KAFKA_MESSAGE_KEY] = msg.raw_message.key

        return attrs

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[ConsumerRecord]",
    ) -> str:
        return cast("str", msg.raw_message.topic)


class BatchKafkaTelemetrySettingsProvider(
    BaseKafkaTelemetrySettingsProvider[tuple["ConsumerRecord", ...]],
):
    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[tuple[ConsumerRecord, ...]]",
    ) -> dict[str, Any]:
        raw_message = msg.raw_message[0]

        return {
            MESSAGING_SYSTEM: self.messaging_system,
            MESSAGING_MESSAGE_ID: msg.message_id,
            MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(
                bytearray().join(cast("Sequence[bytes]", msg.body)),
            ),
            MESSAGING_BATCH_MESSAGE_COUNT: len(msg.raw_message),
            MESSAGING_KAFKA_DESTINATION_PARTITION: raw_message.partition,
            MESSAGING_DESTINATION_PUBLISH_NAME: raw_message.topic,
        }

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[tuple[ConsumerRecord, ...]]",
    ) -> str:
        return cast("str", msg.raw_message[0].topic)


def telemetry_attributes_provider_factory(
    msg: Union["ConsumerRecord", Sequence["ConsumerRecord"], None],
) -> KafkaTelemetrySettingsProvider | BatchKafkaTelemetrySettingsProvider:
    if isinstance(msg, Sequence):
        return BatchKafkaTelemetrySettingsProvider()
    return KafkaTelemetrySettingsProvider()
