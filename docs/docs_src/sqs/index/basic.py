import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker(region_name="us-east-1")
app = FastStream(broker)


@broker.subscriber("my-queue")
async def handler(msg: str) -> None:
    print(msg)


@app.after_startup
async def publish_hello() -> None:
    await broker.publish("Hello, SQS!", "my-queue")


@pytest.mark.asyncio
async def test_basic() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("Hello, SQS!", "my-queue")
        handler.mock.assert_called_once_with("Hello, SQS!")
