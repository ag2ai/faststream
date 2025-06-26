from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker
from faststream.specification.asyncapi import AsyncAPI


class DataBasic(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example"
    )


broker = KafkaBroker("localhost:9092")
app = FastStream(broker)
asyncapi = AsyncAPI(broker, schema_version="3.0.0")


@broker.publisher("output_data")
@broker.subscriber("input_data")
async def on_input_data(msg: DataBasic, logger: Logger) -> DataBasic:
    logger.info(msg)
    return DataBasic(data=msg.data + 1.0)
