from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream
from faststream.kafka import KafkaBroker

class Data(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example"
    )

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

@broker.publisher("input_data")
async def to_input_data(msg: Data) -> Data:
    return Data(data=msg.data+5.0)

@broker.subscriber("input_data")
async def on_input_data(msg: Data):
    pass
