import pytest
from faststream import FastStream, Depends
from faststream.confluent import KafkaBroker, TestKafkaBroker

broker = KafkaBroker("localhost:9092")
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
    broker.provider.override(get_client, get_fake_client)
    try:
        async with TestKafkaBroker(broker) as br:
            response = await br.request({}, "test")
            assert await response.decode() == "fake client"
    finally:
        broker.provider.clear()
