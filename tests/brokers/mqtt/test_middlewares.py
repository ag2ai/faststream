import pytest

from tests.brokers.base.middlewares import MiddlewaresOrderTestcase

from .basic import MQTTMemoryTestcaseConfig


@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestMiddlewaresOrder(MQTTMemoryTestcaseConfig, MiddlewaresOrderTestcase):
    pass
