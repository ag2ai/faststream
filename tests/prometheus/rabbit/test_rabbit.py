from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from faststream.rabbit import RabbitBroker, RabbitExchange
from faststream.rabbit.prometheus.middleware import RabbitPrometheusMiddleware
from tests.brokers.rabbit.test_consume import TestConsume as ConsumeCase
from tests.brokers.rabbit.test_publish import TestPublish as PublishCase
from tests.prometheus.basic import LocalPrometheusTestcase, LocalRPCPrometheusTestcase

from .basic import RabbitPrometheusSettings


@pytest.fixture()
def exchange(queue):
    return RabbitExchange(name=queue)


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestPrometheus(
    RabbitPrometheusSettings,
    LocalPrometheusTestcase,
    LocalRPCPrometheusTestcase,
):
    pass


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestPublishWithPrometheus(PublishCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> RabbitBroker:
        return RabbitBroker(
            middlewares=(RabbitPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestConsumeWithPrometheus(ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> RabbitBroker:
        return RabbitBroker(
            middlewares=(RabbitPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )
