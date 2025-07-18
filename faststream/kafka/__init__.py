from faststream._internal.testing.app import TestApp

try:
    from aiokafka import TopicPartition

    from .annotations import KafkaMessage
    from .broker import KafkaBroker, KafkaPublisher, KafkaRoute, KafkaRouter
    from .response import KafkaPublishCommand, KafkaResponse
    from .testing import TestKafkaBroker

except ImportError as e:
    if "'aiokafka'" not in e.msg:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_KAFKA

    raise ImportError(INSTALL_FASTSTREAM_KAFKA) from e

__all__ = (
    "KafkaBroker",
    "KafkaMessage",
    "KafkaPublishCommand",
    "KafkaPublisher",
    "KafkaResponse",
    "KafkaRoute",
    "KafkaRouter",
    "TestApp",
    "TestKafkaBroker",
    "TopicPartition",
)
