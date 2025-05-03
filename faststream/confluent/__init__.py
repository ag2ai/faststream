try:
    from faststream.testing.app import TestApp

    from .annotations import KafkaMessage
    from .broker import KafkaBroker
    from .response import KafkaResponse
    from .router import KafkaPublisher, KafkaRoute, KafkaRouter
    from .schemas import TopicPartition
    from .testing import TestKafkaBroker

except ImportError as e:
    from faststream.exceptions import INSTALL_FASTSTREAM_CONFLUENT

    raise ImportError(INSTALL_FASTSTREAM_CONFLUENT) from e

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
