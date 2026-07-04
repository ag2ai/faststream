import pytest

from faststream import FastStream
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker()
app = FastStream(broker)


@broker.subscriber(
    "long-jobs",
    visibility_timeout=60,   # message stays invisible for 60s per receive
    extend_visibility=True,  # keep extending it while the handler runs
)
async def handler(msg: str) -> None: ...


@pytest.mark.asyncio
async def test_long_running() -> None:
    async with TestSQSBroker(broker):
        await broker.publish("job", "long-jobs")
        handler.mock.assert_called_once_with("job")
