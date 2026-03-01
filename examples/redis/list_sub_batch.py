from faststream import FastStream, Logger
from faststream.redis import ListSub, RedisBroker

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(list=ListSub("test-list", batch=True))
async def handle(msg: list[str], logger: Logger):
    logger.info(msg)


@app.after_startup
async def t():
    await broker.publish_batch("Hi ", "again, ", "FastStream!", list="test-list")
