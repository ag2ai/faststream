import pytest

from tests.brokers.base.codec import CodecTestcase

from .basic import RedisClusterMemoryTestcaseConfig


@pytest.mark.redis_cluster()
@pytest.mark.asyncio()
class TestClusterCodec(RedisClusterMemoryTestcaseConfig, CodecTestcase):
    pass
