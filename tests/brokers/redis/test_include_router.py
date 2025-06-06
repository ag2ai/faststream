from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import RedisTestcaseConfig


class TestSubscriber(RedisTestcaseConfig, IncludeSubscriberTestcase):
    pass


class TestPublisher(RedisTestcaseConfig, IncludePublisherTestcase):
    pass
