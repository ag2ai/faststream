import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker()
app = FastStream(broker)


@broker.subscriber(
    "my-queue",
    wait_time_seconds=10,   # long-poll wait (0-20)
    max_messages=10,        # messages per receive (1-10)
    visibility_timeout=30,  # per-receive visibility override
)
async def handler(msg: str) -> None: ...


@pytest.mark.asyncio
async def test_subscription() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("hi", "my-queue")
        handler.mock.assert_called_once_with("hi")
