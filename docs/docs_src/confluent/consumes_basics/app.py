from pydantic import BaseModel, Field

from faststream import FastStream, Logger
from faststream.confluent import KafkaBroker


class HelloWorld(BaseModel):
    msg: str = Field(
        ...,
        examples=["Hello"],
        description="Demo hello world message",
    )


broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("hello_world")
async def on_hello_world(msg: HelloWorld, logger: Logger):
    logger.info(msg)
