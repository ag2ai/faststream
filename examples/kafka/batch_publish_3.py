from typing import List

from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("test")
@broker.publisher("response", batch=True)
async def handle():
    return ("hi", "FastStream")


@broker.subscriber("response", batch=True)
async def handle_response(msg: List[str], logger: Logger):
    logger.info(msg)


@app.after_startup
async def test() -> None:
    await broker.publish("hi", "test")
