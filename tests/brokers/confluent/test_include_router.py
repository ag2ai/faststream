from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import ConfluentTestcaseConfig


class TestSubscriber(ConfluentTestcaseConfig, IncludeSubscriberTestcase):
    pass


class TestPublisher(ConfluentTestcaseConfig, IncludePublisherTestcase):
    pass
