import pytest

from tests.brokers.base.codec import CodecTestcase

from .basic import KafkaMemoryTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.asyncio()
class TestKafkaCodec(KafkaMemoryTestcaseConfig, CodecTestcase):
    pass
