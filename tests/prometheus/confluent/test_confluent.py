import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

import pytest
from prometheus_client import CollectorRegistry

from faststream import Context
from faststream.confluent import KafkaBroker
from faststream.confluent.prometheus.middleware import KafkaPrometheusMiddleware
from tests.brokers.confluent.basic import ConfluentTestcaseConfig
from tests.brokers.confluent.test_consume import TestConsume
from tests.brokers.confluent.test_publish import TestPublish
from tests.prometheus.basic import LocalPrometheusTestcase


@pytest.mark.confluent
class TestPrometheus(ConfluentTestcaseConfig, LocalPrometheusTestcase):
    def get_broker(self, apply_types=False, **kwargs):
        return KafkaBroker(apply_types=apply_types, **kwargs)

    def get_middleware(self, **kwargs):
        return KafkaPrometheusMiddleware(**kwargs)

    async def test_metrics_batch(
        self,
        event: asyncio.Event,
        queue: str,
    ):
        middleware = self.get_middleware(registry=CollectorRegistry())
        metrics_manager_mock = Mock()
        middleware._metrics_manager = metrics_manager_mock

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
        self.assert_consume_metrics(
            metrics_manager=metrics_manager_mock, message=message, exception_class=None
        )
        self.assert_publish_metrics(metrics_manager=metrics_manager_mock)


@pytest.mark.confluent
class TestPublishWithPrometheus(TestPublish):
    def get_broker(self, apply_types: bool = False, **kwargs):
        return KafkaBroker(
            middlewares=(KafkaPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.confluent
class TestConsumeWithPrometheus(TestConsume):
    def get_broker(self, apply_types: bool = False, **kwargs):
        return KafkaBroker(
            middlewares=(KafkaPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


class TestKafkaPrometheusMiddleware:
    def test_multiprocess_requires_directory(self):
        with pytest.raises(ValueError):
            KafkaPrometheusMiddleware(
                app_name="test-app",
                multiprocess=True,
                multiprocess_dir=None
            )

    def test_multiprocess_with_directory(self):
        with TemporaryDirectory() as temp_dir:
            middleware = KafkaPrometheusMiddleware(
                app_name="test-app",
                multiprocess=True,
                multiprocess_dir=temp_dir
            )
            assert middleware.registry is not None

    def test_multiprocess_directory_creation(self):
        with TemporaryDirectory() as temp_dir:
            middleware = KafkaPrometheusMiddleware(
                app_name="test-app",
                multiprocess=True,
                multiprocess_dir=temp_dir
            )

            assert Path(temp_dir).exists()

            # In multiprocess mode, the registry should have a MultiProcessCollector
            collectors = list(middleware.registry._collector_to_names.keys())
            assert any("MultiProcessCollector" in str(c) for c in collectors)

    def test_single_process_no_directory(self):
        middleware = KafkaPrometheusMiddleware(
            app_name="test-app",
            multiprocess=False
        )
        assert middleware.registry is not None

        # In single process mode, there should be no MultiProcessCollector
        collectors = list(middleware.registry._collector_to_names.keys())
        assert not any("MultiProcessCollector" in str(c) for c in collectors)

    def test_app_name_labeling(self):
        test_name = "unique-test-app-123"
        middleware = KafkaPrometheusMiddleware(app_name=test_name)
        assert middleware.app_name == test_name
