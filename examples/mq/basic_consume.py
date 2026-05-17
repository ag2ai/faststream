from faststream import FastStream
from faststream.annotations import Logger
from faststream.mq import MQBroker

broker = MQBroker(queue_manager="QM1")
app = FastStream(broker)


@broker.subscriber("test-queue")
async def handle(msg: str, logger: Logger) -> None:
    logger.info(msg)


@app.after_startup
async def test_publishing() -> None:
    await broker.publish("Hello!", "test-queue")
