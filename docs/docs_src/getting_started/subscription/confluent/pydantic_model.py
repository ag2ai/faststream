from pydantic import BaseModel, Field, NonNegativeInt

from faststream import FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


class UserInfo(BaseModel):
    name: str = Field(
        ..., examples=["John"], description="Registered user name",
    )
    user_id: NonNegativeInt = Field(
        ..., examples=[1], description="Registered user id",
    )


@broker.subscriber("test-topic")
async def handle(
    user: UserInfo,
):
    assert user.name == "John"
    assert user.user_id == 1
