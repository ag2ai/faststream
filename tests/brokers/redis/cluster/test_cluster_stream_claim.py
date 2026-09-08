import pytest
from redis.asyncio import RedisCluster

from tests.brokers.redis.basic import RedisClusterTestcaseConfig
from tests.brokers.redis.stream_claim import StreamClaimTestcase


@pytest.mark.connected()
@pytest.mark.redis_cluster()
@pytest.mark.asyncio()
class TestClusterXReadGroupClaim(RedisClusterTestcaseConfig, StreamClaimTestcase):
    client_cls = RedisCluster
