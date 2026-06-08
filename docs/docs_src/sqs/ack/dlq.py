import pytest

from faststream import FastStream
from faststream.sqs import RedrivePolicy, SQSBroker, SQSQueue, TestSQSBroker

broker = SQSBroker()
app = FastStream(broker)

queue = SQSQueue(
    name="orders",
    redrive_policy=RedrivePolicy(
        dead_letter_target_arn="arn:aws:sqs:us-east-1:000000000000:orders-dlq",
        max_receive_count=5,
    ),
)


@broker.subscriber(queue)
async def handler(msg: str) -> None: ...


@pytest.mark.asyncio
async def test_dlq() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("hi", queue)
        handler.mock.assert_called_once_with("hi")
