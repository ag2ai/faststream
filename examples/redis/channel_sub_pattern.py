from faststream import FastStream, Logger, Path
from faststream.redis import PubSub, RedisBroker

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber("logs.{level}")
async def handle_logs(msg: str, logger: Logger, level: str = Path()):
    logger.info("%s: %s", level, msg)


@broker.subscriber(channel=PubSub("test.*", pattern=True))
async def handle_test(msg: str, logger: Logger):
    logger.info(msg)


@app.after_startup
async def t():
    # publish to hanle_logs
    await broker.publish("Hi!", "logs.info")
    # publish to hanle_test
    await broker.publish("Hi!", "test.smth")
