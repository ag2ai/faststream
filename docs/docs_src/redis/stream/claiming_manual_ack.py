from faststream import AckPolicy, FastStream, Logger
from faststream.redis import RedisBroker, RedisStreamMessage, StreamSub, Redis

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(
    stream=StreamSub(
        "critical-tasks",
        group="task-workers",
        consumer="worker-failover",
        min_idle_time=30000,  # 30 seconds
    ),
    ack_policy=AckPolicy.MANUAL,
)
async def handle(msg: RedisStreamMessage, logger: Logger, redis: Redis):
    try:
        # Process the claimed message
        logger.info(f"Processing: {msg.body}")
        # Explicitly acknowledge after successful processing
        await msg.ack(redis=redis, group="critical-tasks")
    except Exception as e:
        # Don't acknowledge - let it be claimed by another consumer
        logger.error(f"Failed to process: {e}")


@app.after_startup
async def publish_test():
    await broker.publish("critical-task-1", stream="critical-tasks")
