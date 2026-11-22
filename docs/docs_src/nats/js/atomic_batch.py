from faststream import FastStream, Logger
from faststream.nats import JStream, NatsBroker

broker = NatsBroker()
app = FastStream(broker)

stream = JStream("orders", allow_atomic=True)


@broker.subscriber("orders.created", stream=stream)
async def handle(msg: dict, logger: Logger):
    logger.info(msg)


@app.after_startup
async def publish_orders():
    await broker.publish_batch(
        {"id": 1},
        {"id": 2},
        {"id": 3},
        subject="orders.created",
        stream="orders",
    )
