import pytest

from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
class TestSubscriber(KafkaTestcaseConfig, IncludeSubscriberTestcase):
    pass


@pytest.mark.kafka()
class TestPublisher(KafkaTestcaseConfig, IncludePublisherTestcase):
    pass
