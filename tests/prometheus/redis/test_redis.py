import asyncio
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from faststream import Context
from faststream.redis import ListSub, RedisBroker
from faststream.redis.prometheus.middleware import RedisPrometheusMiddleware
from tests.brokers.redis.test_consume import TestConsume as ConsumeCase
from tests.brokers.redis.test_publish import TestPublish as PublishCase
from tests.prometheus.basic import LocalPrometheusTestcase, LocalRPCPrometheusTestcase

from .basic import BatchRedisPrometheusSettings, RedisPrometheusSettings


@pytest.mark.connected()
@pytest.mark.redis()
class TestBatchPrometheus(BatchRedisPrometheusSettings, LocalPrometheusTestcase):
    async def test_metrics(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        registry = CollectorRegistry()
        middleware = self.get_middleware(registry=registry)

        broker = self.get_broker(apply_types=True, middlewares=(middleware,))

        args, kwargs = self.get_subscriber_params(list=ListSub(queue, batch=True))

        message = None

        @broker.subscriber(*args, **kwargs)
        async def handler(m=Context("message")):
            event.set()

            nonlocal message
            message = m

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(broker.publish_batch(1, 2, list=queue)),
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


@pytest.mark.connected()
@pytest.mark.redis()
class TestPrometheus(
    RedisPrometheusSettings,
    LocalPrometheusTestcase,
    LocalRPCPrometheusTestcase,
): ...


@pytest.mark.connected()
@pytest.mark.redis()
class TestPublishWithPrometheus(PublishCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> RedisBroker:
        return RedisBroker(
            middlewares=(RedisPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.connected()
@pytest.mark.redis()
class TestConsumeWithPrometheus(ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> RedisBroker:
        return RedisBroker(
            middlewares=(RedisPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )
