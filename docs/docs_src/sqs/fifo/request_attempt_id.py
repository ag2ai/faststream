import pytest

from faststream import FastStream
from faststream.sqs import FifoQueue, SQSBroker, TestSQSBroker

broker = SQSBroker(region_name="us-east-1")
app = FastStream(broker)

orders = FifoQueue(name="orders")


@broker.subscriber(orders, request_attempt_id="attempt-1")
async def handler(msg: str) -> None: ...


@pytest.mark.asyncio
async def test_request_attempt_id() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("ordered-event", orders, group_id="customer-42")
        handler.mock.assert_called_once_with("ordered-event")
