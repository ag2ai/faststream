from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from faststream.specification.asyncapi import AsyncAPI


class DataBasic(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example"
    )


broker = KafkaBroker("localhost:9092")
app = FastStream(broker)
docs_obj = AsyncAPI(
    broker,
    schema_version="2.6.0",
)


@broker.publisher(
    "output_data",
    description="My publisher description",
    title="output_data:Produce",
    schema=DataBasic,
)
@broker.subscriber(
    "input_data", title="input_data:Consume"
)
async def on_input_data(msg):
    """Consumer function

    Args:
        msg: input msg
    """
    # your processing logic
    pass
