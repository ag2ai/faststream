import pytest

from tests.brokers.base.codec import CodecTestcase

from .basic import RabbitMemoryTestcaseConfig


@pytest.mark.rabbit()
@pytest.mark.asyncio()
class TestRabbitCodec(RabbitMemoryTestcaseConfig, CodecTestcase):
    pass
