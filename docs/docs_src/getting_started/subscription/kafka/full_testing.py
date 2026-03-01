import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker

broker = KafkaBroker("localhost:9092")


@broker.subscriber("test-topic")
async def handle(msg: str) -> None:
    raise ValueError


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestKafkaBroker(broker) as br:
        with pytest.raises(ValueError):
            await br.publish("hello!", "test-topic")
