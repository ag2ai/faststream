import pytest

from tests.brokers.base.consume import BrokerConsumeTestcase

from .basic import MQMemoryTestcaseConfig


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestConsume(MQMemoryTestcaseConfig, BrokerConsumeTestcase):
    pass
