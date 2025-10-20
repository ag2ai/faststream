import asyncio

import pytest
from prometheus_client import CollectorRegistry

from faststream import Context
from faststream.kafka import KafkaBroker
from faststream.kafka.prometheus.middleware import KafkaPrometheusMiddleware
from tests.brokers.kafka.test_consume import TestConsume as ConsumeCase
from tests.brokers.kafka.test_publish import TestPublish as PublishCase
from tests.prometheus.basic import LocalPrometheusTestcase

from .basic import BatchKafkaPrometheusSettings, KafkaPrometheusSettings


@pytest.mark.kafka()
@pytest.mark.connected()
class TestBatchPrometheus(BatchKafkaPrometheusSettings, LocalPrometheusTestcase):
    async def test_metrics(
        self,
        queue: str,
    ):
        event = asyncio.Event()

        registry = CollectorRegistry()
        middleware = self.get_middleware(registry=registry)

        broker = self.get_broker(apply_types=True, middlewares=(middleware,))

        args, kwargs = self.get_subscriber_params(queue, batch=True)
        message = None

        @broker.subscriber(*args, **kwargs)
        async def handler(m=Context("message")):
            event.set()

            nonlocal message
            message = m

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(
                    broker.publish_batch("hello", "world", topic=queue),
                ),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        assert event.is_set()
        self.assert_metrics(
            registry=registry,
            message=message,
            exception_class=None,
            custom_labels={},
        )


@pytest.mark.kafka()
@pytest.mark.connected()
class TestPrometheus(KafkaPrometheusSettings, LocalPrometheusTestcase): ...


@pytest.mark.kafka()
@pytest.mark.connected()
class TestPublishWithPrometheus(PublishCase):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs,
    ):
        return KafkaBroker(
            middlewares=(KafkaPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.kafka()
@pytest.mark.connected()
class TestConsumeWithPrometheus(ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs):
        return KafkaBroker(
            middlewares=(KafkaPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )
