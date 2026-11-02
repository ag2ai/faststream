from typing import Any, ClassVar

from faststream.rabbit.prometheus import RabbitPrometheusMiddleware
from faststream.rabbit.prometheus.provider import RabbitMetricsSettingsProvider
from tests.brokers.rabbit.basic import RabbitTestcaseConfig


class RabbitPrometheusSettings(RabbitTestcaseConfig):
    messaging_system = "rabbitmq"
    nack_kwargs: ClassVar[dict[str, Any]] = {"requeue": False}

    def get_middleware(self, **kwargs: Any) -> RabbitPrometheusMiddleware:
        return RabbitPrometheusMiddleware(**kwargs)

    def get_settings_provider(self) -> RabbitMetricsSettingsProvider:
        return RabbitMetricsSettingsProvider()
