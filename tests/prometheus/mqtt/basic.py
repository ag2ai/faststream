from typing import Any

from faststream.mqtt.prometheus import MQTTPrometheusMiddleware
from faststream.mqtt.prometheus.provider import MQTTMetricsSettingsProvider
from tests.brokers.mqtt.basic import MQTTTestcaseConfig


class MQTTPrometheusSettings(MQTTTestcaseConfig):
    messaging_system = "mqtt"

    def get_middleware(self, **kwargs: Any) -> MQTTPrometheusMiddleware:
        return MQTTPrometheusMiddleware(**kwargs)

    def get_settings_provider(self) -> MQTTMetricsSettingsProvider:
        return MQTTMetricsSettingsProvider()
