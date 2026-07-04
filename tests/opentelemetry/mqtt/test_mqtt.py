from typing import Any

import pytest

from faststream.exceptions import FeatureNotSupportedException
from faststream.mqtt import MQTTBroker, MQTTRouter
from faststream.mqtt.opentelemetry import MQTTTelemetryMiddleware
from tests.brokers.mqtt.basic import MQTTTestcaseConfig
from tests.brokers.mqtt.test_consume import TestConsume as ConsumeCase
from tests.brokers.mqtt.test_publish import TestPublish as PublishCase
from tests.opentelemetry.basic import LocalTelemetryTestcase


@pytest.fixture()
def mqtt_version(request: pytest.FixtureRequest) -> str:
    return "5.0"


def test_feature_not_supported_311():
    with pytest.raises(
        FeatureNotSupportedException, match=r"Opentelementry don`t work in 3.1.1 mqtt"
    ):
        MQTTBroker(
            middlewares=(MQTTTelemetryMiddleware(),),
            version="3.1.1",
        )


def test_router_feature_not_supported_311():
    router = MQTTRouter(middlewares=(MQTTTelemetryMiddleware(),))
    with pytest.raises(
        FeatureNotSupportedException, match=r"Opentelementry don`t work in 3.1.1 mqtt"
    ):
        MQTTBroker(
            version="3.1.1",
        ).include_router(router)


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestTelemetry(MQTTTestcaseConfig, LocalTelemetryTestcase):  # type: ignore[misc]
    messaging_system = "mqtt"
    include_messages_counters = True
    telemetry_middleware_class = MQTTTelemetryMiddleware


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestPublishWithTelemetry(PublishCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> MQTTBroker:
        return MQTTBroker(
            middlewares=(MQTTTelemetryMiddleware(),),
            apply_types=apply_types,
            version=self.version,
            **kwargs,
        )


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestConsumeWithTelemetry(ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> MQTTBroker:
        return MQTTBroker(
            middlewares=(MQTTTelemetryMiddleware(),),
            apply_types=apply_types,
            version=self.version,
            **kwargs,
        )
