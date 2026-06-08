from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from faststream.sqs import SQSBroker
from faststream.sqs.prometheus.middleware import SQSPrometheusMiddleware
from tests.brokers.sqs.test_consume import TestConsume as ConsumeCase
from tests.brokers.sqs.test_publish import TestPublish as PublishCase
from tests.prometheus.basic import LocalPrometheusTestcase

from .basic import SQSPrometheusSettings


@pytest.mark.connected()
@pytest.mark.sqs()
class TestPrometheus(SQSPrometheusSettings, LocalPrometheusTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.sqs()
class TestPublishWithPrometheus(PublishCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> SQSBroker:
        broker = super().get_broker(apply_types=apply_types, **kwargs)
        broker.add_middleware(SQSPrometheusMiddleware(registry=CollectorRegistry()))
        return broker


@pytest.mark.connected()
@pytest.mark.sqs()
class TestConsumeWithPrometheus(ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> SQSBroker:
        broker = super().get_broker(apply_types=apply_types, **kwargs)
        broker.add_middleware(SQSPrometheusMiddleware(registry=CollectorRegistry()))
        return broker
