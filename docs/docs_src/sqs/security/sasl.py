import pytest

from faststream import FastStream
from faststream.security import SASLPlaintext
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker(
    region_name="us-east-1",
    security=SASLPlaintext(
        username="AKIA...",
        password="...",
        use_ssl=True,
    ),
)
app = FastStream(broker)


@broker.subscriber("my-queue")
async def handler(msg: str) -> None: ...


@pytest.mark.asyncio
async def test_sasl() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("hi", "my-queue")
        handler.mock.assert_called_once_with("hi")
