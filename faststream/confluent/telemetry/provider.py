from typing import TYPE_CHECKING

from opentelemetry.semconv.trace import SpanAttributes

from faststream.broker.middlewares.telemetry import TelemetrySettingsProvider

if TYPE_CHECKING:
    from confluent_kafka import Message

    from faststream.broker.message import StreamMessage
    from faststream.types import AnyDict


class ConfluentTelemetrySettingsProvider(TelemetrySettingsProvider["Message"]):
    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        self.messaging_system = "kafka"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[Message]",
    ) -> "AnyDict":
        attrs = {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_MESSAGE_ID: msg.message_id,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
            SpanAttributes.MESSAGING_KAFKA_DESTINATION_PARTITION: msg.raw_message.partition(),
            SpanAttributes.MESSAGING_KAFKA_MESSAGE_OFFSET: msg.raw_message.offset(),
            "messaging.destination_publish.name": msg.raw_message.topic(),
        }

        if (key := msg.raw_message.key()) is not None:
            attrs[SpanAttributes.MESSAGING_KAFKA_MESSAGE_KEY] = key

        return attrs

    @staticmethod
    def get_consume_destination_name(
        msg: "StreamMessage[Message]",
    ) -> str:
        return msg.raw_message.topic()

    def get_publish_attrs_from_kwargs(
        self,
        kwargs: "AnyDict",
    ) -> "AnyDict":
        attrs = {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_DESTINATION_NAME: kwargs["topic"],
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: kwargs["correlation_id"],
        }

        if (partition := kwargs["partition"]) is not None:
            attrs[SpanAttributes.MESSAGING_KAFKA_DESTINATION_PARTITION] = partition

        if (key := kwargs["key"]) is not None:
            attrs[SpanAttributes.MESSAGING_KAFKA_MESSAGE_KEY] = key

        return attrs

    @staticmethod
    def get_publish_destination_name(
        kwargs: "AnyDict",
    ) -> str:
        return kwargs["topic"]
