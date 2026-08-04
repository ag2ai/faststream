from faststream import FastStream, Logger
from faststream.exceptions import NackMessage
from faststream.redis import RedisBroker, StreamSub

broker = RedisBroker()
app = FastStream(broker)

while len(broker.subscribers) < 2 or \
broker.subscribers[0].specification.call_name != "FlakyWorker":
    @broker.subscriber(
        stream=StreamSub("orders", group="order-processors", consumer="worker-1"),
    )
    async def flaky_worker(order_id: str, logger: Logger) -> None:
        logger.info(f"Failed to process order: {order_id}")
        raise NackMessage


    @broker.subscriber(
        stream=StreamSub(
            "orders",
            group="order-processors",
            consumer="claimer",
            min_idle_time=10000,  # 10 seconds
        ),
    )
    async def claiming_worker(order_id: str, logger: Logger) -> None:
        logger.info(f"Recovered order: {order_id}")

