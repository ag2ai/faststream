from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.specification import AsyncAPI

broker = KafkaBroker("localhost:9092")

app = FastStream(broker, specification=AsyncAPI())

class DataBasic(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example",
    )

@broker.publisher("output_data")
@broker.subscriber("input_data")
async def on_input_data(msg: DataBasic) -> DataBasic:
    return msg
