import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, TestSQSBroker
from faststream.sqs.annotations import SQSMessage

broker = SQSBroker()
app = FastStream(broker)


@broker.subscriber("my-queue")
async def handler(body: str, msg: SQSMessage) -> None:
    print(msg.message_id, msg.headers, msg.correlation_id)


@pytest.mark.asyncio
async def test_message_info() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("hi", "my-queue")
        handler.mock.assert_called_once_with("hi")
