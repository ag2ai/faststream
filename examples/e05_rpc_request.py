from faststream import FastStream
from faststream.annotations import Logger
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue")
async def handle(msg: str, logger: Logger) -> str:
    logger.info(msg)
    return "pong"


@app.after_startup
async def test_publishing() -> None:
    response = await broker.request("ping", "test-queue")
    assert await response.decode() == "pong"
