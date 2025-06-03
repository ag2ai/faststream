import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

import pytest
from prometheus_client import CollectorRegistry

from faststream import Context
from faststream.redis import ListSub, RedisBroker
from faststream.redis.prometheus.middleware import RedisPrometheusMiddleware
from tests.brokers.redis.test_consume import TestConsume
from tests.brokers.redis.test_publish import TestPublish
from tests.prometheus.basic import LocalPrometheusTestcase


@pytest.mark.redis
class TestPrometheus(LocalPrometheusTestcase):
    def get_broker(self, apply_types=False, **kwargs):
        return RedisBroker(apply_types=apply_types, **kwargs)

    def get_middleware(self, **kwargs):
        return RedisPrometheusMiddleware(**kwargs)

    async def test_metrics_batch(
        self,
        event: asyncio.Event,
        queue: str,
    ):
        middleware = self.get_middleware(registry=CollectorRegistry())
        metrics_manager_mock = Mock()
        middleware._metrics_manager = metrics_manager_mock

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
                asyncio.create_task(broker.publish_batch("hello", "world", list=queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        assert event.is_set()
        self.assert_consume_metrics(
            metrics_manager=metrics_manager_mock, message=message, exception_class=None
        )
        self.assert_publish_metrics(metrics_manager=metrics_manager_mock)


@pytest.mark.redis
class TestPublishWithPrometheus(TestPublish):
    def get_broker(self, apply_types: bool = False, **kwargs):
        return RedisBroker(
            middlewares=(RedisPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.redis
class TestConsumeWithPrometheus(TestConsume):
    def get_broker(self, apply_types: bool = False, **kwargs):
        return RedisBroker(
            middlewares=(RedisPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


class TestKafkaPrometheusMiddleware:
    def test_multiprocess_requires_directory(self):
        with pytest.raises(ValueError):  # noqa: PT011
            RedisPrometheusMiddleware(
                registry=CollectorRegistry(),
                app_name="test-app",
                is_multiprocess=True,
                multiprocess_dir=None,
            )

    def test_multiprocess_with_directory(self):
        with TemporaryDirectory() as temp_dir:
            middleware = RedisPrometheusMiddleware(
                registry=CollectorRegistry(),
                app_name="test-app",
                is_multiprocess=True,
                multiprocess_dir=temp_dir,
            )
            assert middleware._metrics_container._registry is not None

    def test_multiprocess_directory_creation(self):
        with TemporaryDirectory() as temp_dir:
            middleware = RedisPrometheusMiddleware(
                registry=CollectorRegistry(),
                app_name="test-app",
                is_multiprocess=True,
                multiprocess_dir=temp_dir,
            )

            assert Path(temp_dir).exists()

            # In multiprocess mode, the registry should have a MultiProcessCollector
            collectors = list(
                middleware._metrics_container._registry._collector_to_names.keys()
            )
            assert any("MultiProcessCollector" in str(c) for c in collectors)

    def test_single_process_no_directory(self):
        middleware = RedisPrometheusMiddleware(
            registry=CollectorRegistry(), app_name="test-app", is_multiprocess=False
        )
        assert middleware._metrics_container._registry is not None

        # In single process mode, there should be no MultiProcessCollector
        collectors = list(
            middleware._metrics_container._registry._collector_to_names.keys()
        )
        assert not any("MultiProcessCollector" in str(c) for c in collectors)
