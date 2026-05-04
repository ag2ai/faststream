import pytest

from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import RedisClusterTestcaseConfig


@pytest.mark.redis_cluster()
class TestClusterSubscriber(RedisClusterTestcaseConfig, IncludeSubscriberTestcase):
    pass


@pytest.mark.redis_cluster()
class TestClusterPublisher(RedisClusterTestcaseConfig, IncludePublisherTestcase):
    pass
