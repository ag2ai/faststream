from faststream import FastStream, Logger
from faststream.exceptions import NackMessage
from faststream.redis import RedisBroker, StreamSub

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(
    stream=StreamSub("orders", group="order-processors", consumer="worker-1"),
)
async def flaky_worker(order_id: str, logger: Logger) -> None:
    logger.info(f"Failed to process order: {order_id}")
    raise NackMessage
