import pytest

from tests.brokers.base.consume import BrokerRealConsumeTestcase

from .basic import MQTTTestcaseConfig


@pytest.mark.mqtt()
@pytest.mark.connected()
class TestConsume(MQTTTestcaseConfig, BrokerRealConsumeTestcase): ...
