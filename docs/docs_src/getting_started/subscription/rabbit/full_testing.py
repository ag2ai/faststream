import pytest
from faststream.rabbit import RabbitBroker, TestRabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")


@broker.subscriber("test-queue")
async def handle(msg: str) -> None:
    raise ValueError


@pytest.mark.asyncio()
async def test_handle() -> None:
    async with TestRabbitBroker(broker) as br:
        with pytest.raises(ValueError):
            await br.publish("hello!", "test-queue")
