from typing import Any

from faststream import AckPolicy
from faststream.kafka.prometheus import KafkaPrometheusMiddleware
from faststream.kafka.prometheus.provider import (
    BatchKafkaMetricsSettingsProvider,
    KafkaMetricsSettingsProvider,
)
from tests.brokers.kafka.basic import KafkaTestcaseConfig


class BaseKafkaPrometheusSettings(KafkaTestcaseConfig):
    messaging_system = "kafka"

    def get_middleware(self, **kwargs: Any) -> KafkaPrometheusMiddleware:
        return KafkaPrometheusMiddleware(**kwargs)

    def get_subscriber_params(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[
        tuple[Any, ...],
        dict[str, Any],
    ]:
        args, kwargs = super().get_subscriber_params(*args, **kwargs)
        return args, {
            "group_id": "test",
            "ack_policy": AckPolicy.REJECT_ON_ERROR,
            **kwargs,
        }


class KafkaPrometheusSettings(BaseKafkaPrometheusSettings):
    def get_settings_provider(self) -> KafkaMetricsSettingsProvider:
        return KafkaMetricsSettingsProvider()


class BatchKafkaPrometheusSettings(BaseKafkaPrometheusSettings):
    def get_settings_provider(self) -> BatchKafkaMetricsSettingsProvider:
        return BatchKafkaMetricsSettingsProvider()
