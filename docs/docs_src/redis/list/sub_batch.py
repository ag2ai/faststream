from faststream import FastStream, Logger
from faststream.redis import ListSub, RedisBroker

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(list=ListSub("test-list", batch=True))
async def handle(msg: list[str], logger: Logger):
    logger.info(msg)
