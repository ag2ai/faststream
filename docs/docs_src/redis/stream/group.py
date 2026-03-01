from faststream import FastStream, Logger
from faststream.redis import RedisBroker, StreamSub

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(stream=StreamSub("test-stream", group="test-group", consumer="1"))
async def handle(msg: str, logger: Logger):
    logger.info(msg)


@app.after_startup
async def t():
    await broker.publish("Hi!", stream="test-stream")
