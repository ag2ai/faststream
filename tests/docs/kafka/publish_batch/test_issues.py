import pytest

from faststream import FastStream
from faststream.kafka import KafkaBroker, TestKafkaBroker

broker = KafkaBroker()
batch_producer = broker.publisher("response", batch=True)


@batch_producer
@broker.subscriber("test")
async def handle(msg: str) -> list[int]:
    return [1, 2, 3]


app = FastStream(broker)


@pytest.mark.asyncio()
async def test_base_app() -> None:
    async with TestKafkaBroker(broker):
        await broker.publish("", "test")
