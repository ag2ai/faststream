from faststream import FastStream, Logger
from faststream.redis import RedisBroker, StreamSub

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(stream=StreamSub("test-stream", batch=True))
async def handle(msg: list[str], logger: Logger):
    logger.info(msg)


@app.after_startup
async def t():
    await broker.publish("Hi ", stream="test-stream")
    await broker.publish("again, ", stream="test-stream")
    await broker.publish("FastStream!", stream="test-stream")
