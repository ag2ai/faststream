import pytest

from tests.brokers.base.codec import CodecTestcase

from .basic import RedisMemoryTestcaseConfig


@pytest.mark.redis()
@pytest.mark.asyncio()
class TestRedisCodec(RedisMemoryTestcaseConfig, CodecTestcase):
    pass
