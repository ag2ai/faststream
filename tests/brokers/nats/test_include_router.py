from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import NatsTestcaseConfig


class TestSubscriber(NatsTestcaseConfig, IncludeSubscriberTestcase):
    pass


class TestPublisher(NatsTestcaseConfig, IncludePublisherTestcase):
    pass
