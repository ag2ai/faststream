from importlib.util import find_spec
from typing import TYPE_CHECKING, TypeAlias

from faststream._internal.parser import ParserProto
from faststream._internal.testing.app import TestApp

if TYPE_CHECKING:
    from confluent_kafka import Message

ConfluentParserType: TypeAlias = ParserProto["Message"]

try:
    from .annotations import KafkaMessage
    from .broker import KafkaBroker, KafkaPublisher, KafkaRoute, KafkaRouter
    from .helpers.config import ConfluentConfig
    from .response import KafkaPublishCommand, KafkaPublishMessage, KafkaResponse
    from .schemas import Topic, TopicPartition
    from .testing import TestKafkaBroker

except ImportError as e:
    # the package is installed: the failure is its own, not a missing extra
    if find_spec("confluent_kafka") is not None:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_CONFLUENT

    raise ImportError(INSTALL_FASTSTREAM_CONFLUENT) from e

__all__ = (
    "ConfluentConfig",
    "ConfluentParserType",
    "KafkaBroker",
    "KafkaMessage",
    "KafkaPublishCommand",
    "KafkaPublishMessage",
    "KafkaPublisher",
    "KafkaResponse",
    "KafkaRoute",
    "KafkaRouter",
    "TestApp",
    "TestKafkaBroker",
    "Topic",
    "TopicPartition",
)
