from pydantic import Field, NonNegativeInt

from faststream import FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


@broker.subscriber("test-channel")
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
