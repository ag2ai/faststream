from faststream import FastStream
from faststream.annotations import Logger
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue")
@broker.publisher("response-queue")
@broker.subscriber("another-queue")
@broker.publisher("another-response-queue")
async def handle(msg: str, logger: Logger) -> str:
    logger.info(msg)
    return "Response"


@broker.subscriber("response-queue")
async def handle_response_1(msg: str, logger: Logger) -> None:
    logger.info(msg)


@broker.subscriber("another-response-queue")
async def handle_response_2(msg: str, logger: Logger) -> None:
    logger.info(msg)


@app.after_startup
async def test() -> None:
    await broker.publish("Hello!", "test-queue")
    await broker.publish("Hello!", "another-queue")
