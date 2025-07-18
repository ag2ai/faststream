from faststream import FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import Logger, RabbitMessage

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue")
async def handle(
    body: str,
    logger: Logger,
    message: RabbitMessage,
) -> None:
    await message.ack()  # ack first
    logger.info(body)


@app.after_startup
async def test_publishing() -> None:
    await broker.publish("Hello!", "test-queue")
