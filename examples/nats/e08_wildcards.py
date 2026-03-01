from faststream import FastStream, Logger
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStream(broker)


@broker.subscriber("subject.*")
async def handler(msg: str, logger: Logger):
    logger.info(msg)


@broker.subscriber("subject.>")
async def handler_match(msg: str, logger: Logger):
    logger.info(msg)


@app.after_startup
async def test_send():
    await broker.publish("Hi!", "subject.logs")
    await broker.publish("Hi!", "subject.logs.info")
