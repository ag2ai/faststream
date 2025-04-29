from faststream.exceptions import INSTALL_FASTSTREAM_KAFKA

try:
    from aiokafka import TopicPartition
except ImportError as e:
    raise ImportError(INSTALL_FASTSTREAM_KAFKA) from e

from faststream.kafka.annotations import KafkaMessage
from faststream.kafka.broker import KafkaBroker
from faststream.kafka.response import KafkaResponse
from faststream.kafka.router import KafkaPublisher, KafkaRoute, KafkaRouter
from faststream.kafka.testing import TestKafkaBroker
from faststream.testing.app import TestApp

__all__ = (
    "KafkaBroker",
    "KafkaMessage",
    "KafkaPublisher",
    "KafkaResponse",
    "KafkaRoute",
    "KafkaRouter",
    "TestApp",
    "TestKafkaBroker",
    "TopicPartition",
)
