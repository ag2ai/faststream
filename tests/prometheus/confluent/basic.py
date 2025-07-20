from typing import Any

from faststream import AckPolicy
from faststream.confluent.prometheus import KafkaPrometheusMiddleware
from faststream.confluent.prometheus.provider import (
    BatchConfluentMetricsSettingsProvider,
    ConfluentMetricsSettingsProvider,
)
from tests.brokers.confluent.basic import ConfluentTestcaseConfig


class BaseConfluentPrometheusSettings(ConfluentTestcaseConfig):
    messaging_system = "kafka"

    def get_middleware(self, **kwargs: Any) -> KafkaPrometheusMiddleware:
        return KafkaPrometheusMiddleware(**kwargs)

    def get_subscriber_params(
        self,
        *topics: Any,
        **kwargs: Any,
    ) -> tuple[
        tuple[Any, ...],
        dict[str, Any],
    ]:
        topics, kwargs = super().get_subscriber_params(*topics, **kwargs)

        return topics, {
            "group_id": "test",
            "ack_policy": AckPolicy.REJECT_ON_ERROR,
            **kwargs,
        }


class ConfluentPrometheusSettings(BaseConfluentPrometheusSettings):
    def get_settings_provider(self) -> ConfluentMetricsSettingsProvider:
        return ConfluentMetricsSettingsProvider()


class BatchConfluentPrometheusSettings(BaseConfluentPrometheusSettings):
    def get_settings_provider(self) -> BatchConfluentMetricsSettingsProvider:
        return BatchConfluentMetricsSettingsProvider()
