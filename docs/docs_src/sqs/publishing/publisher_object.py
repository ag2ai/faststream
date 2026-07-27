import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker()
app = FastStream(broker)

publisher = broker.publisher("out-queue", headers={"source": "svc-a"})


@publisher
@broker.subscriber("in-queue")
async def handler(msg: str) -> str:
    return msg.upper()  # return value is published to "out-queue"


@pytest.mark.asyncio
async def test_publisher_object() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("hello", "in-queue")
        handler.mock.assert_called_once_with("hello")
        publisher.mock.assert_called_once_with("HELLO")
