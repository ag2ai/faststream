from faststream._internal.testing.app import TestApp

try:
    from .annotations import KafkaMessage
    from .broker import KafkaBroker, KafkaPublisher, KafkaRoute, KafkaRouter
    from .response import KafkaPublishCommand, KafkaPublishMessage, KafkaResponse
    from .schemas import TopicPartition
    from .testing import TestKafkaBroker

except ImportError as e:
    if "'confluent_kafka'" not in e.msg:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_CONFLUENT

    raise ImportError(INSTALL_FASTSTREAM_CONFLUENT) from e

__all__ = (
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
    "TopicPartition",
)
