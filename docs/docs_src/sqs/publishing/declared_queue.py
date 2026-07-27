import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, SQSQueue, TestSQSBroker

broker = SQSBroker()
app = FastStream(broker)

queue = SQSQueue(name="orders", visibility_timeout=60, message_retention_period=86400)


@broker.subscriber(queue)
async def handler(msg: str) -> None:
    print(msg)


async def publish() -> None:
    await broker.publish("data", queue)


@pytest.mark.asyncio
async def test_declared_queue() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("data", queue)
        handler.mock.assert_called_once_with("data")
