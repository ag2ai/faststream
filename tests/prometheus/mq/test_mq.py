import pytest

from tests.marks import require_ibmmq
from tests.prometheus.basic import (
    LocalMetricsSettingsProviderTestcase,
    LocalPrometheusTestcase,
    LocalRPCPrometheusTestcase,
)

from .basic import MQPrometheusSettings


@require_ibmmq
@pytest.mark.mq()
class TestPrometheus(
    MQPrometheusSettings,
    LocalPrometheusTestcase,
    LocalRPCPrometheusTestcase,
    LocalMetricsSettingsProviderTestcase,
):
    pass
