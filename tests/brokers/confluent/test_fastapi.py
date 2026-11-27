import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, call

import pytest
from confluent_kafka import Producer
from pydantic import BaseModel

from faststream.confluent import KafkaRouter
from faststream.confluent.fastapi import (
    KafkaMessage,
    KafkaRouter as StreamRouter,
)
from tests.brokers.base.fastapi import (
    FastAPILocalTestcase,
    FastAPITestcase,
    KafkaTombstoneFastAPILocalTestcase,
)

from .basic import ConfluentMemoryTestcaseConfig, ConfluentTestcaseConfig

if TYPE_CHECKING:
    from .settings import Settings


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

        @router.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def hello(msg: list[str]) -> Any:
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
        mock: MagicMock,
        event: asyncio.Event,
        settings: "Settings",
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/1967."""

        class _BodyModel(BaseModel):
            x: int

        router = self.router_class(bootstrap_servers=settings.url)
        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(msg: _BodyModel | None = None, *, raw: KafkaMessage) -> None:
            raw_message = raw.raw_message
            assert not isinstance(raw_message, tuple)
            mock(msg, raw_message.value())
            if mock.call_count == 2:
                event.set()

        async with router.broker as br:
            await br.start()
            await br.publish(b'{"x": 5}', queue, key=b"k1", partition=0)

            # Send the tombstone from a separate client to exercise the Kafka wire value.
            producer = Producer({"bootstrap.servers": settings.url})
            producer.produce(queue, key=b"k2", value=None, partition=0)
            assert await asyncio.to_thread(producer.flush, self.timeout) == 0
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert mock.call_args_list == [
            call(_BodyModel(x=5), b'{"x": 5}'),
            call(None, None),
        ]


@pytest.mark.confluent()
class TestRouterLocal(
    ConfluentMemoryTestcaseConfig,
    FastAPILocalTestcase,
    KafkaTombstoneFastAPILocalTestcase,
):
    router_class = StreamRouter
    broker_router_class = KafkaRouter

    @pytest.mark.parametrize(
        ("payload", "expected_body", "expected_value"),
        (
            pytest.param("hi", ["hi"], b"hi", id="text"),
            pytest.param(None, [b""], None, id="tombstone"),
        ),
    )
    async def test_batch_testclient(
        self,
        mock: MagicMock,
        queue: str,
        payload: str | None,
        expected_body: list[str | bytes],
        expected_value: bytes | None,
    ) -> None:
        router = self.router_class()
        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @router.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def hello(msg: list[str | bytes], *, raw: KafkaMessage) -> None:
            records = raw.raw_message
            assert isinstance(records, tuple)
            mock(msg, [record.value() for record in records], raw.tombstone)

        async with self.patch_broker(router.broker) as br:
            await br.publish(payload, queue, key=b"k")

        mock.assert_called_once_with(expected_body, [expected_value], False)
