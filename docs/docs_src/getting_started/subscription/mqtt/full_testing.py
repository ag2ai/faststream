import pytest
from faststream.mqtt import MQTTBroker, TestMQTTBroker

broker = MQTTBroker("localhost", port=1883)


@broker.subscriber("test-topic")
async def handle(msg: str) -> None:
    raise ValueError


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestMQTTBroker(broker) as br:
        with pytest.raises(ValueError):
            await br.publish("hello!", "test-topic")
