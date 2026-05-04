import pytest

from tests.brokers.base.consume import BrokerRealConsumeTestcase

from .basic import RedisClusterTestcaseConfig


@pytest.mark.connected()
@pytest.mark.redis_cluster()
class TestClusterConsume(RedisClusterTestcaseConfig, BrokerRealConsumeTestcase):
    """Standard broker consume tests running against a real Redis Cluster."""
