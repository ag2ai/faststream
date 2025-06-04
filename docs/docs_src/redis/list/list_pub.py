from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream
from faststream.redis import RedisBroker


class Data(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example"
    )


broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


@broker.subscriber(list="input-list")
@broker.publisher(list="output-list")
async def on_input_data(msg: Data) -> Data:
    return Data(data=msg.data + 1.0)
