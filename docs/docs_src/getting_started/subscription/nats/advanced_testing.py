from typing import Annotated

from pydantic import BaseModel
import pytest

from faststream import Context, FastStream
from faststream.nats import NatsBroker, TestNatsBroker

broker = NatsBroker()
app = FastStream(broker)


class Data(BaseModel):
    name: str
    user_id: int


@broker.subscriber("test.subject")
async def handle(
    data: Data,
    subject: Annotated[str, Context("message.raw_message.subject")],
) -> None:
    assert data.name == "John"
    assert data.user_id == 1
    assert subject == "test.subject"


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestNatsBroker(broker) as br:
        await br.publish(Data(name="John", user_id=1), subject="test.subject")

        await handle.assert_called_once_with(
            {"name": "John", "user_id": 1},
            context={
                "message.raw_message.subject": "test.subject",
            },
        )
        # or
        await handle.assert_called_once_with(
            Data(name="John", user_id=1),
            context={
                "message.raw_message.subject": "test.subject",
            },
        )
