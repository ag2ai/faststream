from typing import Annotated

from pydantic import BaseModel
import pytest

from faststream import Context, FastStream
from faststream.rabbit import RabbitBroker, TestRabbitBroker

broker = RabbitBroker()
app = FastStream(broker)


class Data(BaseModel):
    name: str
    user_id: int


@broker.subscriber("test-queue")
async def handle(
    data: Data,
    queue: Annotated[str, Context("message.raw_message.routing_key")],
) -> None:
    assert data.name == "John"
    assert data.user_id == 1
    assert queue == "test-queue"


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestRabbitBroker(broker) as br:
        await br.publish(Data(name="John", user_id=1), queue="test-queue")

        await handle.assert_called_once_with(
            {"name": "John", "user_id": 1},
            context={
                "message.raw_message.routing_key": "test-queue",
            },
        )
        # or
        await handle.assert_called_once_with(
            Data(name="John", user_id=1),
            context={
                "message.raw_message.routing_key": "test-queue",
            },
        )
