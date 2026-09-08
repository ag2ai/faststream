from typing import Annotated

from pydantic import BaseModel
import pytest

from faststream import FastStream, Header
from faststream.rabbit import RabbitBroker, TestRabbitBroker

broker = RabbitBroker()
app = FastStream(broker)


class Data(BaseModel):
    name: str
    user_id: int


@broker.subscriber("test-queue")
async def handle(
    data: Data,
    trace_id: Annotated[str, Header("trace-id")],
) -> None:
    assert data.name == "John"
    assert data.user_id == 1
    assert trace_id == "42"


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestRabbitBroker(broker) as br:
        await br.publish(
            Data(name="John", user_id=1),
            queue="test-queue",
            headers={"trace-id": "42"},
        )

        await handle.assert_called_once_with(
            {"name": "John", "user_id": 1},
            headers={"trace-id": "42"},
        )
        # or
        await handle.assert_called_once_with(
            Data(name="John", user_id=1),
            headers={"trace-id": "42"},
        )
