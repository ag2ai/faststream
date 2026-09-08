from faststream import FastStream, Logger
from faststream.redis import RedisBroker, StreamSub

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(stream=StreamSub("orders", no_ack=True))
async def fire_and_forget_worker(order_id: str, logger: Logger) -> None:
    logger.info(f"Processing order: {order_id}")
    error_msg = f"Could not process order: {order_id}"
    raise ValueError(error_msg)
