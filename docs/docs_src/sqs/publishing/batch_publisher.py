import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker()
app = FastStream(broker)

publisher = broker.publisher("target-queue", batch=True)


@broker.subscriber("target-queue")
async def handler(msg: int) -> None: ...


async def send() -> None:
    await publisher.publish(1, 2, 3)  # one SendMessageBatch request


@pytest.mark.asyncio
async def test_batch_publisher() -> None:
    async with TestSQSBroker(broker):
        await publisher.publish(1, 2, 3)
        assert handler.mock.call_count == 3
