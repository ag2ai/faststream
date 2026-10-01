from importlib.util import find_spec
from typing import TYPE_CHECKING, Any, TypeAlias

from faststream._internal.parser import ParserProto
from faststream._internal.testing.app import TestApp

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord

KafkaParserType: TypeAlias = ParserProto["ConsumerRecord[Any, Any]"]

try:
    from aiokafka import ConsumerRecord
    from aiokafka.structs import RecordMetadata

    from .annotations import KafkaMessage
    from .broker import KafkaBroker, KafkaPublisher, KafkaRoute, KafkaRouter
    from .response import KafkaPublishCommand, KafkaPublishMessage, KafkaResponse
    from .schemas import TopicPartition
    from .testing import TestKafkaBroker

except ImportError as e:
    # the package is installed: the failure is its own, not a missing extra
    if find_spec("aiokafka") is not None:
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
