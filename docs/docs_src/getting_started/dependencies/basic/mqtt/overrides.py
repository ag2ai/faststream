import pytest
from faststream import FastStream, Depends
from faststream.mqtt import MQTTBroker, TestMQTTBroker

broker = MQTTBroker("localhost", port=1883)
app = FastStream(broker)

def get_client() -> str:
    return "real client"

@broker.subscriber("test")
async def handler(body: dict, client: str = Depends(get_client)) -> str:
    return client

def get_fake_client() -> str:
    return "fake client"

@pytest.mark.asyncio
async def test_handler() -> None:
    with broker.provider.scope(get_client, get_fake_client):
        async with TestMQTTBroker(broker) as br:
            response = await br.request({}, "test")
            assert await response.decode() == "fake client"
