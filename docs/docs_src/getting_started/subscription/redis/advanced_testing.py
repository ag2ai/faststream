from typing import Annotated

from pydantic import BaseModel
import pytest

from faststream import Context, FastStream
from faststream.redis import RedisBroker, TestRedisBroker

broker = RedisBroker()
app = FastStream(broker)


class Data(BaseModel):
    name: str
    user_id: int


@broker.subscriber("test-channel")
async def handle(
    data: Data,
    channel: Annotated[str, Context("message.raw_message.channel")],
) -> None:
    assert data.name == "John"
    assert data.user_id == 1
    assert channel == "test-channel"


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestRedisBroker(broker) as br:
        await br.publish(Data(name="John", user_id=1), channel="test-channel")

        await handle.assert_called_once_with(
            {"name": "John", "user_id": 1},
            context={
                "message.raw_message.channel": "test-channel",
            },
        )
        # or
        await handle.assert_called_once_with(
            Data(name="John", user_id=1),
            context={
                "message.raw_message.channel": "test-channel",
            },
        )
