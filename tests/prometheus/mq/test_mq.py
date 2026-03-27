import pytest

from tests.prometheus.basic import (
    LocalMetricsSettingsProviderTestcase,
    LocalPrometheusTestcase,
    LocalRPCPrometheusTestcase,
)

from .basic import MQPrometheusSettings


@pytest.mark.mq()
class TestPrometheus(
    MQPrometheusSettings,
    LocalPrometheusTestcase,
    LocalRPCPrometheusTestcase,
    LocalMetricsSettingsProviderTestcase,
):
    pass
