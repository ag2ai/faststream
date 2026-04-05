import pytest

from tests.brokers.base.consume import BrokerRealConsumeTestcase

from .basic import MQTTTestcaseConfig


@pytest.mark.connected()
@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestConsume(MQTTTestcaseConfig, BrokerRealConsumeTestcase):
    async def test_consume_with_filter(self, queue, mock):
        if self.version == "3.1.1":
            pytest.skip("content_type filtering not supported in MQTT 3.1.1")
        await super().test_consume_with_filter(queue, mock)
