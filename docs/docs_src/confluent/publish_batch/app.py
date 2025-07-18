from typing import Tuple

from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream, Logger
from faststream.confluent import KafkaBroker


class Data(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example",
    )


broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


decrease_and_increase = broker.publisher("output_data", batch=True)


@decrease_and_increase
@broker.subscriber("input_data_1")
async def on_input_data_1(msg: Data, logger: Logger) -> Tuple[Data, Data]:
    logger.info(msg)
    return Data(data=(msg.data * 0.5)), Data(data=(msg.data * 2.0))


@broker.subscriber("input_data_2")
async def on_input_data_2(msg: Data, logger: Logger) -> None:
    logger.info(msg)
    await decrease_and_increase.publish(
        Data(data=(msg.data * 0.5)), Data(data=(msg.data * 2.0)),
    )
