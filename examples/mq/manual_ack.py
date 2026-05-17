from faststream import FastStream
from faststream.middlewares import AckPolicy
from faststream.mq import MQBroker
from faststream.mq.annotations import Logger, MQMessage

broker = MQBroker(queue_manager="QM1")
app = FastStream(broker)


@broker.subscriber("test-queue", ack_policy=AckPolicy.MANUAL)
async def handle(body: str, logger: Logger, message: MQMessage) -> None:
    await message.ack()
    logger.info(body)


@app.after_startup
async def test_publishing() -> None:
    await broker.publish("Hello!", "test-queue")
