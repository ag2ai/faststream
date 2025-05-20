from typing import List

from pydantic import BaseModel, Field

from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


class HelloWorld(BaseModel):
    msg: str = Field(
        ...,
        examples=["Hello"],
        description="Demo hello world message",
    )


@broker.subscriber("test_batch", batch=True)
async def handle_batch(msg: list[HelloWorld], logger: Logger):
    logger.info(msg)
