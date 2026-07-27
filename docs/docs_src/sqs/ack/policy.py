import pytest

from faststream import AckPolicy, FastStream
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker(region_name="us-east-1", ack_policy=AckPolicy.NACK_ON_ERROR)
app = FastStream(broker)


@broker.subscriber("my-queue")
async def handler(msg: str) -> None: ...


@pytest.mark.asyncio
async def test_policy() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("hi", "my-queue")
        handler.mock.assert_called_once_with("hi")
