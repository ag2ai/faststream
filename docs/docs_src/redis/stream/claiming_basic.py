from faststream import FastStream, Logger
from faststream.redis import RedisBroker, StreamSub

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(
    stream=StreamSub(
        "orders",
        group="processors",
        consumer="worker-1",
        min_idle_time=5000,  # Claim messages idle for 5+ seconds
    )
)
async def handle(order_id: str, logger: Logger):
    logger.info(f"Processing order: {order_id}")


@app.after_startup
async def publish_test():
    await broker.publish("order-123", stream="orders")
