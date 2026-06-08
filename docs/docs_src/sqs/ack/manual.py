import pytest

from faststream import AckPolicy, FastStream
from faststream.sqs import SQSBroker, TestSQSBroker
from faststream.sqs.annotations import SQSMessage

broker = SQSBroker()
app = FastStream(broker)


@broker.subscriber("my-queue", ack_policy=AckPolicy.MANUAL)
async def handler(body: str, msg: SQSMessage) -> None:
    await msg.ack()    # DeleteMessage
    # or: await msg.nack()  / await msg.reject()


@pytest.mark.asyncio
async def test_manual() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("hi", "my-queue")
        handler.mock.assert_called_once_with("hi")
