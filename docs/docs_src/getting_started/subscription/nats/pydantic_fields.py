from pydantic import Field, NonNegativeInt

from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)


@broker.subscriber("test-subject")
async def handle(
    name: str = Field(
        ..., examples=["John"], description="Registered user name",
    ),
    user_id: NonNegativeInt = Field(
        ..., examples=[1], description="Registered user id",
    ),
):
    assert name == "John"
    assert user_id == 1
