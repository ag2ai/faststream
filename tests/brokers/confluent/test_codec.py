import pytest

from tests.brokers.base.codec import CodecTestcase

from .basic import ConfluentMemoryTestcaseConfig


@pytest.mark.confluent()
@pytest.mark.asyncio()
class TestConfluentCodec(ConfluentMemoryTestcaseConfig, CodecTestcase):
    pass
