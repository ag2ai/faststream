import asyncio
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from faststream import Context
from faststream.confluent import KafkaBroker
from faststream.confluent.prometheus.middleware import KafkaPrometheusMiddleware
from tests.brokers.confluent.test_consume import TestConsume
from tests.brokers.confluent.test_publish import TestPublish
from tests.prometheus.basic import LocalPrometheusTestcase

from .basic import BatchConfluentPrometheusSettings, ConfluentPrometheusSettings


@pytest.mark.confluent()
class TestBatchPrometheus(BatchConfluentPrometheusSettings, LocalPrometheusTestcase):
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
                    broker.publish_batch("hello", "world", topic=queue)
                ),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        assert event.is_set()
        self.assert_metrics(
            registry=registry,
            message=message,
            exception_class=None,
        )


@pytest.mark.confluent()
class TestPrometheus(ConfluentPrometheusSettings, LocalPrometheusTestcase):
    pass


@pytest.mark.confluent()
class TestPublishWithPrometheus(TestPublish):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> KafkaBroker:
        return KafkaBroker(
            middlewares=(KafkaPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.confluent()
class TestConsumeWithPrometheus(TestConsume):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> KafkaBroker:
        return KafkaBroker(
            middlewares=(KafkaPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )
