from faststream import FastStream, Logger
from faststream.redis import RedisBroker, StreamSub

broker = RedisBroker()
app = FastStream(broker)
stream = StreamSub(
    "test-stream",
    group="test-group",
    consumer="1",
    maxlen=1000,
)


@broker.subscriber(stream=stream)
async def handle(msg: str, logger: Logger) -> None:
    logger.info(msg)


@app.after_startup
async def t() -> None:
    await broker.publish("Hi!", stream=stream)
