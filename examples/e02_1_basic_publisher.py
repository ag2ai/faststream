from faststream import FastStream
from faststream.annotations import Logger
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue")
@broker.publisher("response-queue")
async def handle(msg, logger: Logger):
    logger.info(msg)
    return "Response"


@broker.subscriber("response-queue")
async def handle_response(msg, logger: Logger):
    logger.info("Process response: %s", msg)


@app.after_startup
async def test_publishing():
    await broker.publish("Hello!", "test-queue")
