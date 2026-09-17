import asyncio
from unittest.mock import MagicMock

import pytest

from faststream import Context
from faststream.kafka import KafkaRouter
from faststream.kafka.fastapi import (
    KafkaMessage,
    KafkaRouter as StreamRouter,
)
from tests.brokers.base.fastapi import (
    FastAPILocalTestcase,
    FastAPITestcase,
    KafkaTombstoneFastAPILocalTestcase,
    _Foo,
)

from .basic import KafkaMemoryTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.connected()
class TestKafkaRouter(FastAPITestcase):
    router_class = StreamRouter
    broker_router_class = KafkaRouter

    async def test_batch_real(
        self, mock: MagicMock, queue: str, event: asyncio.Event
    ) -> None:
        router = self.router_class()

        @router.subscriber(queue, batch=True)
        async def hello(msg: list[str]):
            event.set()
            return mock(msg)

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hi", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
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

        @router.subscriber(queue)
        async def handler(
            msg: _Foo | None = None,
            raw: KafkaMessage = Context("message"),
        ) -> None:
            received.append((msg, raw.raw_message.value))
            if len(received) == 2:
                event.set()

        async with router.broker as br:
            await br.start()

            await br.publish(b'{"x": 5}', queue, key=b"k1")
            # bypass the encoder to prove this works for a tombstone
            # produced by any client, not only faststream's publish(None)
            await br._producer._producer.producer.send(
                topic=queue,
                key=b"k2",
                value=None,
            )

            await asyncio.wait_for(event.wait(), timeout=3)

        assert (_Foo(x=5), b'{"x": 5}') in received
        assert (None, None) in received


@pytest.mark.kafka()
class TestRouterLocal(
    KafkaMemoryTestcaseConfig,
    FastAPILocalTestcase,
    KafkaTombstoneFastAPILocalTestcase,
):
    router_class = StreamRouter
    broker_router_class = KafkaRouter

    async def test_group_instance_id(self) -> None:
        router = self.router_class()

        sub = router.subscriber(
            "test-topic",
            group_id="test-group",
            group_instance_id="instance-4",
        )

        assert sub._connection_args["group_instance_id"] == "instance-4"

    async def test_batch_testclient(
        self, mock: MagicMock, queue: str, event: asyncio.Event
    ) -> None:
        router = self.router_class()

        @router.subscriber(queue, batch=True)
        async def hello(msg: list[str]):
            event.set()
            return mock(msg)

        async with self.patch_broker(router.broker) as br:
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hi", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])
