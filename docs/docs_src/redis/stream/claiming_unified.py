from faststream import FastStream, Logger
from faststream.redis import RedisBroker, StreamSub
from faststream.redis.annotations import RedisStreamMessage

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(
    stream=StreamSub(
        "orders",
        group="processors",
        consumer="worker-1",
        claim_min_idle_time=5000,  # Also claim messages idle for 5+ seconds
    )
)
async def handle(order_id: str, message: RedisStreamMessage, logger: Logger):
    # Previous deliveries: 0 for new messages, 1+ for reclaimed ones
    if message.raw_message["delivery_counts"][0]:
        idle_ms = message.raw_message["idle_times"][0]
        logger.info(f"Recovering order {order_id} after {idle_ms} ms idle")
    else:
        logger.info(f"Processing order: {order_id}")


@app.after_startup
async def publish_test():
    await broker.publish("order-123", stream="orders")
