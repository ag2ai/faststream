from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from faststream.mqtt import MQTTBroker
from faststream.mqtt.prometheus.middleware import MQTTPrometheusMiddleware
from tests.brokers.mqtt.test_consume import TestConsume as ConsumeCase
from tests.brokers.mqtt.test_publish import TestPublish as PublishCase
from tests.prometheus.basic import LocalPrometheusTestcase, LocalRPCPrometheusTestcase

from .basic import MQTTPrometheusSettings


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestPrometheus(
    MQTTPrometheusSettings,
    LocalPrometheusTestcase,
    LocalRPCPrometheusTestcase,
):
    async def test_rpc_request(self, queue: str) -> None:
        if self.version == "3.1.1":
            pytest.skip(
                "request/reply not supported in MQTT 3.1.1 without explicit reply_to",
            )
        await super().test_rpc_request(queue)


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestPublishWithPrometheus(PublishCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> MQTTBroker:
        return MQTTBroker(
            middlewares=(MQTTPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            version=self.version,
            **kwargs,
        )


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestConsumeWithPrometheus(ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> MQTTBroker:
        return MQTTBroker(
            middlewares=(MQTTPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            version=self.version,
            **kwargs,
        )
