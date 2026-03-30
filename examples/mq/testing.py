import pytest

from faststream import FastStream
from faststream.mq import MQBroker, TestMQBroker

broker = MQBroker(queue_manager="QM1")
app = FastStream(broker)


@broker.subscriber("test-queue")
async def handle(msg: str) -> None:
    raise ValueError


@pytest.mark.asyncio()
async def test_handle() -> None:
    async with TestMQBroker(broker) as br:
        with pytest.raises(ValueError):  # noqa: PT011
            await br.publish("hello!", "test-queue")
