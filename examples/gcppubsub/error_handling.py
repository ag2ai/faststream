import pytest

from faststream import FastStream
from faststream.gcppubsub import GCPPubSubBroker, TestGCPPubSubBroker

broker = GCPPubSubBroker(project_id="test-project")
app = FastStream(broker)


@broker.subscriber("error-queue", topic="errors")
async def handle(msg: str) -> None:
    if msg == "error":
        raise ValueError("Processing failed")


@pytest.mark.asyncio()
async def test_handle() -> None:
    async with TestGCPPubSubBroker(broker) as br:
        with pytest.raises(ValueError):  # noqa: PT011
            await br.publish("error", "errors")