import pytest

from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import RedisTestcaseConfig


@pytest.mark.redis()
class TestSubscriber(RedisTestcaseConfig, IncludeSubscriberTestcase):
    pass


@pytest.mark.redis()
class TestPublisher(RedisTestcaseConfig, IncludePublisherTestcase):
    pass
