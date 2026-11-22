import asyncio
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock, call

import pytest
from pydantic import BaseModel

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
)

from .basic import ConfluentMemoryTestcaseConfig, ConfluentTestcaseConfig

if TYPE_CHECKING:
    from faststream.confluent.publisher.producer import AsyncConfluentFastProducerImpl


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
    ) -> None:
        class _BodyModel(BaseModel):
            x: int

        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(
            msg: _BodyModel | None = None,
            raw: KafkaMessage = Context("message"),
        ) -> None:
            raw_message = (
                raw.raw_message[0]
                if isinstance(raw.raw_message, tuple)
                else raw.raw_message
            )
            mock(msg, raw_message.value)
            if mock.call_count == 2:
                event.set()

        async with self.patch_broker(router.broker) as br:
            await br.start()

            producer = cast("AsyncConfluentFastProducerImpl", br._producer)

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(b'{"x": 5}', queue, key=b"k1")),
                    # bypass the encoder to prove this works for a tombstone
                    # produced by any client, not only faststream's publish(None)
                    asyncio.create_task(
                        producer._producer.producer.send(
                            topic=queue, key=b"k2", value=None
                        )
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

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

    async def test_batch_testclient(
        self, mock: MagicMock, queue: str, event: asyncio.Event
    ) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @router.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def hello(msg: list[str]) -> Any:
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
