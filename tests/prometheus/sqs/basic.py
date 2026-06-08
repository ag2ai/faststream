from typing import Any

from faststream.sqs.prometheus import SQSPrometheusMiddleware
from faststream.sqs.prometheus.provider import SQSMetricsSettingsProvider
from tests.brokers.sqs.basic import SQSTestcaseConfig


class SQSPrometheusSettings(SQSTestcaseConfig):
    messaging_system = "aws_sqs"

    def get_middleware(self, **kwargs: Any) -> SQSPrometheusMiddleware:
        return SQSPrometheusMiddleware(**kwargs)

    def get_settings_provider(self) -> SQSMetricsSettingsProvider:
        return SQSMetricsSettingsProvider()
