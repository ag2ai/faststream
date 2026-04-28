from typing import TYPE_CHECKING

from faststream._internal.testing.app import TestApp

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord

    from faststream._internal.parser import ParserProto

    KafkaParserType = ParserProto["ConsumerRecord"]

try:
    from aiokafka import ConsumerRecord, TopicPartition
    from aiokafka.structs import RecordMetadata

    from .annotations import KafkaMessage
    from .broker import KafkaBroker, KafkaPublisher, KafkaRoute, KafkaRouter
    from .response import KafkaPublishCommand, KafkaPublishMessage, KafkaResponse
    from .testing import TestKafkaBroker

except ImportError as e:
    if "'aiokafka'" not in e.msg:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_KAFKA

    raise ImportError(INSTALL_FASTSTREAM_KAFKA) from e

__all__ = (
    "ConsumerRecord",
    "KafkaBroker",
    "KafkaMessage",
    "KafkaParserType",
    "KafkaPublishCommand",
    "KafkaPublishMessage",
    "KafkaPublisher",
    "KafkaResponse",
    "KafkaRoute",
    "KafkaRouter",
    "RecordMetadata",
    "TestApp",
    "TestKafkaBroker",
    "TopicPartition",
)
