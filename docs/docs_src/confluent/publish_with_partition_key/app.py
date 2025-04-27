from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import Context, FastStream, Logger
from faststream.confluent import KafkaBroker


class Data(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example"
    )


broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


to_output_data = broker.publisher("output_data")


@broker.subscriber("input_data")
async def on_input_data(
    msg: Data, logger: Logger, key: bytes = Context("message.raw_message.key")
) -> None:
    logger.info("on_input_data(msg=%s)", msg)
    await to_output_data.publish(Data(data=msg.data + 1.0), key=b"key")


@broker.subscriber("output_data")
async def on_output_data(
    msg: Data, logger: Logger, key: bytes = Context("message.raw_message.key")
) -> None:
    logger.info("on_output_data(msg=%s)", msg)
