import pytest

from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import MQTTTestcaseConfig

_SKIP_V311 = "not supported in MQTT 3.1.1"


@pytest.mark.connected()
@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestPublish(MQTTTestcaseConfig, BrokerPublishTestcase):
    async def test_response(self, queue, mock):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_response(queue, mock)

    async def test_reply_to(self, queue, mock):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_reply_to(queue, mock)
