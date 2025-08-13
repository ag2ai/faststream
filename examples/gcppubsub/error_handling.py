import pytest

from faststream import FastStream
from faststream.gcp import GCPBroker, TestGCPBroker

broker = GCPBroker(project_id="test-project")
app = FastStream(broker)


@broker.subscriber("error-queue", topic="errors")
async def handle(msg: str) -> None:
    if msg == "error":
        raise ValueError("Processing failed")


@pytest.mark.asyncio()
async def test_handle() -> None:
    async with TestGCPBroker(broker) as br:
        with pytest.raises(ValueError):  # noqa: PT011
            await br.publish("error", "errors")
