import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker(region_name="us-east-1", response_queue="responses")
app = FastStream(broker)


@broker.subscriber("echo")
async def echo(msg: str) -> str:
    return f"reply: {msg}"


async def call() -> None:
    response = await broker.request("ping", "echo", timeout=10.0)
    assert await response.decode() == "reply: ping"


@pytest.mark.asyncio
async def test_rpc() -> None:
    async with TestSQSBroker(broker):
        response = await broker.request("ping", "echo", timeout=10.0)
        assert await response.decode() == "reply: ping"
