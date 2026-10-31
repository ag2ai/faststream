from faststream import FastStream, Logger
from faststream.redis import ListSub, RedisBroker

broker = RedisBroker()
app = FastStream(broker)
list_sub = ListSub("test-list", batch=True)


@broker.subscriber(list=list_sub)
async def handle(msg: list[str], logger: Logger) -> None:
    logger.info(msg)


@app.after_startup
async def publish_messages() -> None:
    await broker.publish_batch("Hi", "again", list=list_sub)
