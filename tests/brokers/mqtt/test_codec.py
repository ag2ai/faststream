import pytest

from tests.brokers.base.codec import CodecTestcase

from .basic import MQTTMemoryTestcaseConfig


@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestMQTTCodec(MQTTMemoryTestcaseConfig, CodecTestcase):
    pass
