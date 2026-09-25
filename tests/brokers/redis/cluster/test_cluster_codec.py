import pytest

from tests.brokers.base.codec import CodecTestcase
from tests.brokers.redis.basic import RedisClusterMemoryTestcaseConfig


@pytest.mark.redis_cluster()
@pytest.mark.asyncio()
class TestClusterCodec(RedisClusterMemoryTestcaseConfig, CodecTestcase):
    pass
