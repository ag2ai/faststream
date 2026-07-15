from typing import Any, Literal

import pytest

from faststream.mqtt.broker.broker import MQTTBroker
from tests.brokers.base.connection import BrokerConnectionTestcase

from .conftest import Settings


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestConnection(BrokerConnectionTestcase):
    broker = MQTTBroker
    version: Literal["5.0", "3.1.1"] = "3.1.1"

    @pytest.fixture(autouse=True)
    def setup_version(self, mqtt_version: Literal["5.0", "3.1.1"]) -> None:
        self.version = mqtt_version

    def get_broker_args(self, settings: Settings) -> dict[str, Any]:
        return {
            "host": settings.host,
            "port": settings.port,
            "version": self.version,
        }
