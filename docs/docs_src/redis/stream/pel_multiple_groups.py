from faststream import FastStream, Logger
from faststream.exceptions import NackMessage
from faststream.redis import RedisBroker, StreamSub

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(
    stream=StreamSub("orders", group="billing", consumer="worker-1"),
)
async def billing_worker(order_id: str, logger: Logger) -> None:
    logger.info(f"Billing failed for order: {order_id}")
    raise NackMessage


@broker.subscriber(
    stream=StreamSub("orders", group="shipping", consumer="worker-1"),
)
async def shipping_worker(order_id: str, logger: Logger) -> None:
    logger.info(f"Shipping failed for order: {order_id}")
    raise NackMessage
