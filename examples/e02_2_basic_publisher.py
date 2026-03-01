from faststream import FastStream
from faststream.annotations import Logger
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)

publisher = broker.publisher("response-queue")


@publisher
@broker.subscriber("test-queue")
async def handle(msg: str, logger: Logger) -> str:
    logger.info(msg)
    return "Response"


@broker.subscriber("response-queue")
async def handle_response(msg: str, logger: Logger) -> None:
    logger.info("Process response: %s", msg)


@app.after_startup
async def test_publishing() -> None:
    await broker.publish("Hello!", "test-queue")
