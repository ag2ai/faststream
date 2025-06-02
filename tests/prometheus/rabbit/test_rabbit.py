from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from prometheus_client import CollectorRegistry

from faststream.rabbit import RabbitBroker, RabbitExchange
from faststream.rabbit.prometheus.middleware import RabbitPrometheusMiddleware
from tests.brokers.rabbit.test_consume import TestConsume
from tests.brokers.rabbit.test_publish import TestPublish
from tests.prometheus.basic import LocalPrometheusTestcase


@pytest.fixture
def exchange(queue):
    return RabbitExchange(name=queue)


@pytest.mark.rabbit
class TestPrometheus(LocalPrometheusTestcase):
    def get_broker(self, apply_types=False, **kwargs):
        return RabbitBroker(apply_types=apply_types, **kwargs)

    def get_middleware(self, **kwargs):
        return RabbitPrometheusMiddleware(**kwargs)


@pytest.mark.rabbit
class TestPublishWithPrometheus(TestPublish):
    def get_broker(self, apply_types: bool = False, **kwargs):
        return RabbitBroker(
            middlewares=(RabbitPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.rabbit
class TestConsumeWithPrometheus(TestConsume):
    def get_broker(self, apply_types: bool = False, **kwargs):
        return RabbitBroker(
            middlewares=(RabbitPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


class TestKafkaPrometheusMiddleware:
    def test_multiprocess_requires_directory(self):
        with pytest.raises(ValueError):
            RabbitPrometheusMiddleware(
                app_name="test-app",
                multiprocess=True,
                multiprocess_dir=None
            )

    def test_multiprocess_with_directory(self):
        with TemporaryDirectory() as temp_dir:
            middleware = RabbitPrometheusMiddleware(
                app_name="test-app",
                multiprocess=True,
                multiprocess_dir=temp_dir
            )
            assert middleware.registry is not None

    def test_multiprocess_directory_creation(self):
        with TemporaryDirectory() as temp_dir:
            middleware = RabbitPrometheusMiddleware(
                app_name="test-app",
                multiprocess=True,
                multiprocess_dir=temp_dir
            )

            assert Path(temp_dir).exists()

            # In multiprocess mode, the registry should have a MultiProcessCollector
            collectors = list(middleware.registry._collector_to_names.keys())
            assert any("MultiProcessCollector" in str(c) for c in collectors)

    def test_single_process_no_directory(self):
        middleware = RabbitPrometheusMiddleware(
            app_name="test-app",
            multiprocess=False
        )
        assert middleware.registry is not None

        # In single process mode, there should be no MultiProcessCollector
        collectors = list(middleware.registry._collector_to_names.keys())
        assert not any("MultiProcessCollector" in str(c) for c in collectors)

    def test_app_name_labeling(self):
        test_name = "unique-test-app-123"
        middleware = RabbitPrometheusMiddleware(app_name=test_name)
        assert middleware.app_name == test_name
