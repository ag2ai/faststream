import pytest
from pydantic import ValidationError

from faststream import FastStream
from faststream.rabbit import RabbitBroker, TestRabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@pytest.mark.asyncio()
async def test_correct() -> None:
    async with TestRabbitBroker(broker) as br:
        await br.publish(...)


@pytest.mark.asyncio()
async def test_invalid() -> None:
    async with TestRabbitBroker(broker) as br:  # noqa: F841
        with pytest.raises(ValidationError):
            ...
