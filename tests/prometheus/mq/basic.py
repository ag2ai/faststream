from typing import Any

from faststream.mq.prometheus import MQPrometheusMiddleware
from faststream.mq.prometheus.provider import MQMetricsSettingsProvider
from faststream.prometheus import MetricsSettingsProvider
from tests.brokers.mq.basic import MQMemoryTestcaseConfig


class MQPrometheusSettings(MQMemoryTestcaseConfig):
    messaging_system = "ibm_mq"

    def get_middleware(self, **kwargs: Any) -> MQPrometheusMiddleware:
        return MQPrometheusMiddleware(**kwargs)

    def get_settings_provider(self) -> MetricsSettingsProvider[Any]:
        return MQMetricsSettingsProvider()
