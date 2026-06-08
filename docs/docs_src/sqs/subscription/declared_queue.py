import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, SQSQueue, TestSQSBroker

broker = SQSBroker()
app = FastStream(broker)


@broker.subscriber(SQSQueue(name="orders", visibility_timeout=60))
async def handler(msg: str) -> None: ...


@pytest.mark.asyncio
async def test_declared_queue() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("hi", "orders")
        handler.mock.assert_called_once_with("hi")
