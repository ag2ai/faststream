import asyncio
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from faststream import Context
from faststream.confluent import KafkaRouter
from faststream.confluent.fastapi import (
    KafkaMessage,
    KafkaRouter as StreamRouter,
)
from tests.brokers.base.fastapi import FastAPILocalTestcase, FastAPITestcase

from .basic import ConfluentMemoryTestcaseConfig, ConfluentTestcaseConfig


class _Foo(BaseModel):
    x: int


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

    async def test_optional_body_resolves_to_none_for_tombstone(
        self,
        queue: str,
    ) -> None:
        """Optional body param should resolve to None for a real tombstone.

        Not crash-loop on required fields.
        """
        router = self.router_class()
        received: list[tuple[object, bytes | None]] = []

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def handler(
            msg: _Foo | None = None,
            raw: KafkaMessage = Context("message"),
        ) -> None:
            received.append((msg, raw.raw_message.value()))

        async with self.patch_broker(router.broker) as br:
            await br.start()

            await br.publish(b'{"x": 5}', queue, key=b"k1")

            # bypass the encoder to construct a genuine null value directly -
            # same technique the maintainer used to repro ag2ai/faststream#1967,
            # since this test must stand on its own without depending on the
            # separate publish(None, ...) producer-side fix
            raw_producer = br._producer._producer.producer
            await raw_producer.send(topic=queue, key=b"k2", value=None)

            await asyncio.sleep(self.timeout)

        assert received == [
            (_Foo(x=5), b'{"x": 5}'),
            (None, None),
        ]


@pytest.mark.confluent()
class TestRouterLocal(ConfluentMemoryTestcaseConfig, FastAPILocalTestcase):
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
