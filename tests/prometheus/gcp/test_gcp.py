import pytest
from prometheus_client import CollectorRegistry

from faststream.gcp import GCPBroker
from faststream.gcp.prometheus.middleware import GCPPrometheusMiddleware
from tests.brokers.gcp.test_consume import TestConsume as ConsumeCase
from tests.brokers.gcp.test_publish import TestPublish as PublishCase
from tests.prometheus.basic import LocalPrometheusTestcase

from .basic import BaseGCPPrometheusSettings, GCPPrometheusSettings


@pytest.mark.gcp()
@pytest.mark.connected()
class TestPrometheus(GCPPrometheusSettings, LocalPrometheusTestcase):
    pass


@pytest.mark.gcp()
@pytest.mark.connected()
class TestPublishWithPrometheus(BaseGCPPrometheusSettings, PublishCase):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs,
    ):
        return GCPBroker(
            project_id="test-project",
            middlewares=(GCPPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.gcp()
@pytest.mark.connected()
class TestConsumeWithPrometheus(BaseGCPPrometheusSettings, ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs):
        return GCPBroker(
            project_id="test-project",
            middlewares=(GCPPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )
