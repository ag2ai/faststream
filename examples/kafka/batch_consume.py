from typing import List

from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("test", batch=True)
async def handle(msg: List[str], logger: Logger):
    logger.info(msg)


@app.after_startup
async def test() -> None:
    await broker.publish_batch("hi", "FastStream", topic="test")
