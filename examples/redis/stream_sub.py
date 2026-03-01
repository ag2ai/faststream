from faststream import FastStream, Logger
from faststream.redis import RedisBroker

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(stream="test-stream")
async def handle(msg: str, logger: Logger):
    logger.info(msg)


@app.after_startup
async def t():
    await broker.publish("Hi!", stream="test-stream")
