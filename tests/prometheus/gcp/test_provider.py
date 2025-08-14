import pytest

from tests.prometheus.basic import LocalMetricsSettingsProviderTestcase

from .basic import GCPPrometheusSettings


@pytest.mark.gcp()
class TestGCPMetricsSettingsProvider(
    GCPPrometheusSettings, LocalMetricsSettingsProviderTestcase
):
    pass
