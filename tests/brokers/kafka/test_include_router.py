from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import KafkaTestcaseConfig


class TestSubscriber(KafkaTestcaseConfig, IncludeSubscriberTestcase):
    pass


class TestPublisher(KafkaTestcaseConfig, IncludePublisherTestcase):
    pass
