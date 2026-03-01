from faststream import FastStream, Logger
from faststream.redis import PubSub, RedisBroker

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(channel=PubSub("test.*", pattern=True))
async def handle_test(msg: str, logger: Logger):
    logger.info(msg)
