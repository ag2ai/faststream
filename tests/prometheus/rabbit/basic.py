from typing import Any

from faststream.prometheus import MetricsSettingsProvider
from faststream.rabbit.prometheus import RabbitPrometheusMiddleware
from faststream.rabbit.prometheus.provider import RabbitMetricsSettingsProvider
from tests.brokers.rabbit.basic import RabbitTestcaseConfig


class RabbitPrometheusSettings(RabbitTestcaseConfig):
    messaging_system = "rabbitmq"

    def get_middleware(self, **kwargs: Any) -> RabbitPrometheusMiddleware:
        return RabbitPrometheusMiddleware(**kwargs)

    def get_settings_provider(self) -> MetricsSettingsProvider[Any]:
        return RabbitMetricsSettingsProvider()
