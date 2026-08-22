import asyncio
from unittest.mock import MagicMock

import pytest

from faststream import Context
from faststream.confluent import KafkaRouter
from faststream.confluent.fastapi import (
    KafkaMessage,
    KafkaRouter as StreamRouter,
)
from tests.brokers.base.fastapi import (
    FastAPILocalTestcase,
    FastAPITestcase,
    KafkaTombstoneFastAPILocalTestcase,
    _Foo,
)

from .basic import ConfluentMemoryTestcaseConfig, ConfluentTestcaseConfig


@pytest.mark.connected()
@pytest.mark.confluent()
class TestConfluentRouter(ConfluentTestcaseConfig, FastAPITestcase):
    router_class = StreamRouter
    broker_router_class = KafkaRouter

    async def test_batch_real(
        self, mock: MagicMock, queue: str, event: asyncio.Event
    ) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: list[str]):
            event.set()
            return mock(msg)

        async with self.patch_broker(router.broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hi", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])

    async def test_external_tombstone_resolves_to_none(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        router = self.router_class()
        received: list[tuple[object, bytes | None]] = []

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def handler(
            msg: _Foo | None = None,
            raw: KafkaMessage = Context("message"),
        ) -> None:
            received.append((msg, raw.raw_message.value()))
            if len(received) == 2:
                event.set()

        async with self.patch_broker(router.broker) as br:
            await br.start()

            await br.publish(b'{"x": 5}', queue, key=b"k1")
            # bypass the encoder to prove this works for a tombstone
            # produced by any client, not only faststream's publish(None)
            await br._producer._producer.producer.send(topic=queue, key=b"k2", value=None)

            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert (_Foo(x=5), b'{"x": 5}') in received
        assert (None, None) in received


@pytest.mark.confluent()
class TestRouterLocal(
    ConfluentMemoryTestcaseConfig,
    FastAPILocalTestcase,
    KafkaTombstoneFastAPILocalTestcase,
):
    router_class = StreamRouter
    broker_router_class = KafkaRouter

    async def test_batch_testclient(
        self, mock: MagicMock, queue: str, event: asyncio.Event
    ) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: list[str]):
            event.set()
            return mock(msg)

        async with self.patch_broker(router.broker) as br:
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hi", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])
