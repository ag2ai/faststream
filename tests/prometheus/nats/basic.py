from typing import Any

from faststream.nats.prometheus import NatsPrometheusMiddleware
from faststream.nats.prometheus.provider import (
    BatchNatsMetricsSettingsProvider,
    NatsMetricsSettingsProvider,
)
from tests.brokers.nats.basic import NatsTestcaseConfig


class BaseNatsPrometheusSettings(NatsTestcaseConfig):
    messaging_system = "nats"

    def get_middleware(self, **kwargs: Any) -> NatsPrometheusMiddleware:
        return NatsPrometheusMiddleware(**kwargs)


class NatsPrometheusSettings(BaseNatsPrometheusSettings):
    def get_settings_provider(self) -> NatsMetricsSettingsProvider:
        return NatsMetricsSettingsProvider()


class BatchNatsPrometheusSettings(BaseNatsPrometheusSettings):
    def get_settings_provider(self) -> BatchNatsMetricsSettingsProvider:
        return BatchNatsMetricsSettingsProvider()
