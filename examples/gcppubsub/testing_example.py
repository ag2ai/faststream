import pytest

from faststream import FastStream
from faststream.gcp import GCPBroker, TestGCPBroker

broker = GCPBroker(project_id="test-project")
app = FastStream(broker)


@broker.subscriber("test-subscription", topic="test-topic")
async def handle(msg: str):
    return f"Response: {msg}"


@pytest.mark.asyncio()
async def test_handle():
    async with TestGCPBroker(broker) as br:
        result = await br.publish("Hello!", topic="test-topic")
        assert result is not None
        print("✅ Basic publish test passed!")
