from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import RabbitTestcaseConfig


class TestSubscriber(RabbitTestcaseConfig, IncludeSubscriberTestcase):
    pass


class TestPublisher(RabbitTestcaseConfig, IncludePublisherTestcase):
    pass
