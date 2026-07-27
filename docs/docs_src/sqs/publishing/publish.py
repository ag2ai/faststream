import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker()
app = FastStream(broker)


@broker.subscriber("my-queue")
async def handler(msg: str) -> None:
    print(msg)


async def publish() -> None:
    await broker.publish(
        "payload",
        "my-queue",
        headers={"trace-id": "abc"},
        delay_seconds=5,
    )


@pytest.mark.asyncio
async def test_publish() -> None:
    async with TestSQSBroker(broker):
        await broker.publish(
            "payload",
            "my-queue",
            headers={"trace-id": "abc"},
        )
        handler.mock.assert_called_once_with("payload")
