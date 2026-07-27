import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker(
    region_name="us-east-1",
    aws_access_key_id="AKIA...",
    aws_secret_access_key="...",
    aws_session_token="...",  # optional, for temporary credentials
)
app = FastStream(broker)


@broker.subscriber("my-queue")
async def handler(msg: str) -> None: ...


@pytest.mark.asyncio
async def test_explicit() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("hi", "my-queue")
        handler.mock.assert_called_once_with("hi")
