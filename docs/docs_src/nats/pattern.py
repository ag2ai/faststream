from faststream import FastStream, Logger
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStream(broker)

@broker.subscriber("*.info", "workers")
async def base_handler1(logger: Logger):
    logger.info("base_handler1")

@broker.subscriber("*.info", "workers")
async def base_handler2(logger: Logger):  # pragma: no branch
    logger.info("base_handler2")

@broker.subscriber("*.error", "workers")
async def base_handler3(logger: Logger):
    logger.info("base_handler3")

@app.after_startup
async def send_messages():
    await broker.publish("", "logs.info")  # handlers: 1 or 2
    await broker.publish("", "logs.info")  # handlers: 1 or 2
    await broker.publish("", "logs.error") # handlers: 3
