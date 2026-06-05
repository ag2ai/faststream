import asyncio

import pytest

from faststream.sqs import FifoQueue, SQSBroker, SQSQueue
from tests.marks import require_sqs

from .conftest import Settings


def _broker(settings: Settings) -> SQSBroker:
    return SQSBroker(
        endpoint_url=settings.endpoint_url,
        region_name=settings.region_name,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )


@require_sqs
@pytest.mark.sqs()
@pytest.mark.connected()
@pytest.mark.asyncio()
class TestSQSConnect:
    async def test_ping(self, settings: Settings) -> None:
        broker = _broker(settings)
        async with broker:
            assert await broker.ping(timeout=5.0)

    async def test_publish_consume_ack(self, settings: Settings, queue: str) -> None:
        broker = _broker(settings)
        event = asyncio.Event()
        received: list[str] = []

        @broker.subscriber(SQSQueue(name=queue))
        async def handler(msg: str) -> None:
            received.append(msg)
            event.set()

        async with broker:
            await broker.start()
            await broker.publish("hello", queue)
            await asyncio.wait_for(event.wait(), timeout=15.0)

        assert received == ["hello"]

    async def test_fifo(self, settings: Settings, queue: str) -> None:
        broker = _broker(settings)
        event = asyncio.Event()
        received: list[str] = []

        fifo = FifoQueue(name=queue, content_based_deduplication=True)

        @broker.subscriber(fifo)
        async def handler(msg: str) -> None:
            received.append(msg)
            event.set()

        async with broker:
            await broker.start()
            await broker.publish("ordered", fifo, group_id="group-1")
            await asyncio.wait_for(event.wait(), timeout=15.0)

        assert received == ["ordered"]
