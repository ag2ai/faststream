import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker()
app = FastStream(broker)


@broker.subscriber("my-queue")
async def handler(msg: str) -> None:
    print(msg)


async def publish() -> None:
    await broker.publish_batch("a", "b", "c", queue="my-queue")


@pytest.mark.asyncio
async def test_batch() -> None:
    async with TestSQSBroker(broker):
        await broker.publish_batch("a", "b", "c", queue="my-queue")
        assert handler.mock.call_count == 3
