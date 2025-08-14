import uuid
from typing import Any

import pytest

from faststream.gcp.prometheus import GCPPrometheusMiddleware
from faststream.gcp.prometheus.provider import GCPMetricsSettingsProvider
from tests.brokers.gcp.basic import GCPTestcaseConfig


class BaseGCPPrometheusSettings(GCPTestcaseConfig):
    messaging_system = "gcp_pubsub"

    def get_middleware(self, **kwargs: Any) -> GCPPrometheusMiddleware:
        return GCPPrometheusMiddleware(**kwargs)

    @pytest.fixture()
    def queue(self) -> str:
        """Generate GCP-compatible topic name without dashes."""
        return f"testtopic{uuid.uuid4().hex[:8]}"

    @pytest.fixture()
    def subscription(self) -> str:
        """Generate GCP-compatible subscription name without dashes."""
        return f"testsub{uuid.uuid4().hex[:8]}"

    @pytest.fixture()
    def topic(self) -> str:
        """Generate GCP-compatible topic name without dashes."""
        return f"testtopic{uuid.uuid4().hex[:8]}"


class GCPPrometheusSettings(BaseGCPPrometheusSettings):
    def get_settings_provider(self) -> GCPMetricsSettingsProvider:
        return GCPMetricsSettingsProvider()
