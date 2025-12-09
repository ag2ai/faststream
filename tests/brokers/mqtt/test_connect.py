import pytest

from faststream.mqtt import MQTTBroker
from tests.brokers.base.connection import BrokerConnectionTestcase


@pytest.mark.mqtt()
@pytest.mark.connected()
class TestConnection(BrokerConnectionTestcase):
    broker = MQTTBroker
