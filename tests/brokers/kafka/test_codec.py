import pytest

from tests.brokers.base.codec import BatchCodecTestcase, CodecTestcase

from .basic import KafkaMemoryTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.asyncio()
class TestKafkaCodec(KafkaMemoryTestcaseConfig, CodecTestcase):
    pass


@pytest.mark.kafka()
@pytest.mark.asyncio()
class TestKafkaBatchCodec(KafkaMemoryTestcaseConfig, BatchCodecTestcase):
    pass
