try:
    from aiokafka import TopicPartition

    from faststream.testing.app import TestApp

    from .annotations import KafkaMessage
    from .broker import KafkaBroker
    from .response import KafkaResponse
    from .router import KafkaPublisher, KafkaRoute, KafkaRouter
    from .testing import TestKafkaBroker

except ImportError as e:
    from faststream.exceptions import INSTALL_FASTSTREAM_KAFKA

    raise ImportError(INSTALL_FASTSTREAM_KAFKA) from e

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
