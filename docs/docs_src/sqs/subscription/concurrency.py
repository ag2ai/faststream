import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker()
app = FastStream(broker)


@broker.subscriber("jobs", max_workers=5)
async def handler(msg: str) -> None: ...


@pytest.mark.asyncio
async def test_concurrency() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("job", "jobs")
        handler.mock.assert_called_once_with("job")
