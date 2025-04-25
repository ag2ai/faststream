import asyncio
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


@pytest.mark.confluent
class TestConfluentPrometheus(ConfluentTestcaseConfig, LocalPrometheusTestcase):
    def get_broker(self, apply_types=False, **kwargs):
        config = get_confluent_config()
        return KafkaBroker(
            producer=AsyncConfluentProducer(**config),
            consumer=AsyncConfluentConsumer("test-topic", **config),
            apply_types=apply_types,
            **kwargs
        )

    def get_middleware(self, **kwargs):
        return KafkaPrometheusMiddleware(**kwargs)

    async def test_metrics_single_message(
        self,
        event: asyncio.Event,
        queue: str,
    ):
        middleware = self.get_middleware(registry=CollectorRegistry())
        metrics_manager_mock = Mock()
        middleware._metrics_manager = metrics_manager_mock

        broker = self.get_broker(
            apply_types=True,
            middlewares=(middleware,),
        )

        message = None

        @broker.subscriber(queue)
        async def handler(m=Context()):
            event.set()
            nonlocal message
            message = m

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(broker.publish("ping!", topic=queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        assert event.is_set()
        self.assert_consume_metrics(
            metrics_manager=metrics_manager_mock,
            message=message,
            exception_class=None,
        )
        self.assert_publish_metrics(metrics_manager=metrics_manager_mock)


@pytest.mark.confluent
class TestConfluentPublish(TestPublish):
    def get_broker(self, apply_types: bool = False, **kwargs):
        config = get_confluent_config()
        return KafkaBroker(
            producer=AsyncConfluentProducer(**config),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.confluent
class TestConfluentConsume(TestConsume):
    def get_broker(self, apply_types: bool = False, **kwargs):
        config = get_confluent_config()
        return KafkaBroker(
            consumer=AsyncConfluentConsumer("test-topic", **config),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.confluent
class TestConfluentPrometheusPublish(TestPublish):
    def get_broker(self, apply_types: bool = False, **kwargs):
        config = get_confluent_config()
        return KafkaBroker(
            producer=AsyncConfluentProducer(**config),
            middlewares=(KafkaPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.confluent
class TestConfluentPrometheusConsume(TestConsume):
    def get_broker(self, apply_types: bool = False, **kwargs):
        config = get_confluent_config()
        return KafkaBroker(
            consumer=AsyncConfluentConsumer("test-topic", **config),
            middlewares=(KafkaPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )
