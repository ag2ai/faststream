import pytest

from tests.brokers.base.codec import BatchCodecTestcase, CodecTestcase

from .basic import ConfluentMemoryTestcaseConfig


@pytest.mark.confluent()
@pytest.mark.asyncio()
class TestConfluentCodec(ConfluentMemoryTestcaseConfig, CodecTestcase):
    pass


@pytest.mark.confluent()
@pytest.mark.asyncio()
class TestConfluentBatchCodec(ConfluentMemoryTestcaseConfig, BatchCodecTestcase):
    pass
