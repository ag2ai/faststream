import pytest

from tests.brokers.base.codec import CodecTestcase

from .basic import NatsMemoryTestcaseConfig


@pytest.mark.nats()
@pytest.mark.asyncio()
class TestNatsCodec(NatsMemoryTestcaseConfig, CodecTestcase):
    pass
