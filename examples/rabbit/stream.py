from faststream import FastStream, Logger
from faststream.rabbit import RabbitBroker, RabbitQueue, Channel

broker = RabbitBroker(default_channel=Channel(prefetch_count=10))
app = FastStream(broker)

queue = RabbitQueue(
    name="stream-test",
    durable=True,
    arguments={
        "x-queue-type": "stream",
    },
)


@broker.subscriber(
    queue,
    consume_args={"x-stream-offset": "first"},
)
async def handle(msg, logger: Logger) -> None:
    logger.info(msg)


@app.after_startup
async def test() -> None:
    await broker.publish("Hi!", queue)
