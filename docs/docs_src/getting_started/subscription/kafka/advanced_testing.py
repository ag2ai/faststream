from typing import Annotated

from pydantic import BaseModel
import pytest

from faststream import Context, FastStream
from faststream.kafka import KafkaBroker, TestKafkaBroker

broker = KafkaBroker()
app = FastStream(broker)


class Data(BaseModel):
    name: str
    user_id: int


@broker.subscriber("test-topic")
async def handle(
    data: Data,
    topic: Annotated[str, Context("message.raw_message.topic")],
) -> None:
    assert data.name == "John"
    assert data.user_id == 1
    assert topic == "test-topic"


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestKafkaBroker(broker) as br:
        await br.publish(Data(name="John", user_id=1), topic="test-topic")

        await handle.assert_called_once_with(
            {"name": "John", "user_id": 1},
            context={
                "message.raw_message.topic": "test-topic",
            },
        )
        # or
        await handle.assert_called_once_with(
            Data(name="John", user_id=1),
            context={
                "message.raw_message.topic": "test-topic",
            },
        )
