import pytest

from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import MQMemoryTestcaseConfig


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestPublish(MQMemoryTestcaseConfig, BrokerPublishTestcase):
    pass
