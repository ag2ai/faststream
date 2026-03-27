import pytest

from faststream.mq.broker.router import MQPublisher, MQRoute
from tests.brokers.base.router import RouterTestcase

from .basic import MQMemoryTestcaseConfig


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestRouter(MQMemoryTestcaseConfig, RouterTestcase):
    route_class = MQRoute
    publisher_class = MQPublisher
