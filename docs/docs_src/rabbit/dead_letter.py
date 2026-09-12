from faststream import FastStream, Logger
from faststream.exceptions import RejectMessage
from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitMessage,
    RabbitQueue,
)

broker = RabbitBroker()
app = FastStream(broker)

dead_letter_exchange = RabbitExchange("orders-dlx", type=ExchangeType.DIRECT)
dead_letter_queue = RabbitQueue("orders-dead", routing_key="orders")

orders_queue = RabbitQueue(
    "orders",
    arguments={
        "x-dead-letter-exchange": "orders-dlx",
        "x-dead-letter-routing-key": "orders",
        "x-message-ttl": 60_000,
    },
)


@broker.subscriber(orders_queue)
async def handle_order(order_id: str, logger: Logger) -> None:
    if order_id.startswith("bad"):
        raise RejectMessage

    logger.info("processed %s", order_id)


@broker.subscriber(dead_letter_queue, dead_letter_exchange)
async def handle_dead_letter(
    order_id: str,
    msg: RabbitMessage,
    logger: Logger,
) -> None:
    reason = msg.headers["x-death"][0]["reason"]
    logger.warning("order %s dead-lettered: %s", order_id, reason)
