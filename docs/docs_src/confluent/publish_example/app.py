from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream
from faststream.confluent import KafkaBroker


class Data(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example",
    )


broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


to_output_data = broker.publisher("output_data")


@to_output_data
@broker.subscriber("input_data")
async def on_input_data(msg: Data) -> Data:
    return Data(data=msg.data + 1.0)
