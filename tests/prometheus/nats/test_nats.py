import asyncio
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from faststream import Context
from faststream.nats import JStream, NatsBroker, NatsMessage, PullSub
from faststream.nats.prometheus.middleware import NatsPrometheusMiddleware
from tests.brokers.nats.test_consume import TestConsume as ConsumeCase
from tests.brokers.nats.test_publish import TestPublish as PublishCase
from tests.prometheus.basic import LocalPrometheusTestcase, LocalRPCPrometheusTestcase

from .basic import BatchNatsPrometheusSettings, NatsPrometheusSettings


@pytest.fixture()
def stream(queue: str) -> Any:
    return JStream(queue)


@pytest.mark.connected()
@pytest.mark.nats()
class TestBatchPrometheus(BatchNatsPrometheusSettings, LocalPrometheusTestcase):
    async def test_metrics(  # type: ignore[override]
        self, queue: str, stream: JStream, event: asyncio.Event
    ) -> None:
        registry = CollectorRegistry()
        middleware = self.get_middleware(registry=registry)

        broker = self.get_broker(apply_types=True, middlewares=(middleware,))

        args, kwargs = self.get_subscriber_params(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True, timeout=None),
        )
        message: NatsMessage | None = None

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(m: NatsMessage = Context("message")) -> None:
            nonlocal message
            message = m
            event.set()

        async with broker:
            await broker.start()
            await broker.publish("hello", queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert message is not None
        assert await message.decode() == ["hello"]
        self.assert_metrics(
            registry=registry,
            message=message,
            exception_class=None,
            custom_labels={},
        )

    async def test_batch_publish_metrics(self, queue: str, event: asyncio.Event) -> None:
        registry = CollectorRegistry()
        middleware = self.get_middleware(registry=registry)

        broker = self.get_broker(apply_types=True, middlewares=(middleware,))

        args, kwargs = self.get_subscriber_params(
            queue,
            stream=JStream(queue, allow_atomic=True),
            pull_sub=PullSub(2, batch=True, timeout=None),
        )
        message: NatsMessage | None = None

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(m: NatsMessage = Context("message")) -> None:
            nonlocal message
            message = m
            event.set()

        async with broker:
            await broker.start()
            await broker.publish_batch("hello", "world", subject=queue, stream=queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert message is not None
        assert await message.decode() == ["hello", "world"]
        self.assert_metrics(
            registry=registry,
            message=message,
            exception_class=None,
            custom_labels={},
        )


@pytest.mark.connected()
@pytest.mark.nats()
class TestPrometheus(
    NatsPrometheusSettings,
    LocalRPCPrometheusTestcase,
): ...


@pytest.mark.connected()
@pytest.mark.nats()
class TestPublishWithPrometheus(PublishCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> NatsBroker:
        return NatsBroker(
            middlewares=(NatsPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )


@pytest.mark.connected()
@pytest.mark.nats()
class TestConsumeWithPrometheus(ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> NatsBroker:
        return NatsBroker(
            middlewares=(NatsPrometheusMiddleware(registry=CollectorRegistry()),),
            apply_types=apply_types,
            **kwargs,
        )
