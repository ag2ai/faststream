from typing import Any, Literal

import pytest

from faststream.mqtt.broker.broker import MQTTBroker
from faststream.mqtt.broker.router import MQTTRouter
from faststream.mqtt.testing import TestMQTTBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class MQTTTestcaseConfig(BaseTestcaseConfig):
    version: Literal["5.0", "3.1.1"] = "3.1.1"

    @pytest.fixture(autouse=True)
    def setup_version(self, mqtt_version: Literal["5.0", "3.1.1"]) -> None:
        self.version = mqtt_version

    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> MQTTBroker:
        return MQTTBroker(apply_types=apply_types, version=self.version, **kwargs)

    def patch_broker(self, broker: MQTTBroker, **kwargs: Any) -> MQTTBroker:
        return broker

    def get_router(self, **kwargs: Any) -> MQTTRouter:
        return MQTTRouter(**kwargs)


class MQTTMemoryTestcaseConfig(MQTTTestcaseConfig):
    def patch_broker(
        self,
        broker: MQTTBroker,
        **kwargs: Any,
    ) -> TestMQTTBroker:
        return TestMQTTBroker(broker, **kwargs)
