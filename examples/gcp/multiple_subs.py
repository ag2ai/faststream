from faststream import FastStream, Logger
from faststream.gcp import GCPBroker

broker = GCPBroker(project_id="test-project")
app = FastStream(broker)


@broker.subscriber("events-sub-1", topic="events")
async def handle1(msg: str, logger: Logger):
    logger.info(f"Handler 1: {msg}")


@broker.subscriber("events-sub-2", topic="events")
async def handle2(msg: str, logger: Logger):
    logger.info(f"Handler 2: {msg}")


@broker.subscriber("orders-sub", topic="orders")
async def handle_orders(msg: str, logger: Logger):
    logger.info(f"Order handler: {msg}")


@app.after_startup
async def test_send():
    await broker.publish("User signup", "events")  # Both handlers 1 & 2
    await broker.publish("New order", "orders")   # Order handler only
