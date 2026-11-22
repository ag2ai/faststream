from faststream import FastStream, Logger
from faststream.rabbit import RabbitBroker

orders_broker = RabbitBroker(virtualhost="orders")
billing_broker = RabbitBroker(virtualhost="billing")

app = FastStream(orders_broker, billing_broker)


@orders_broker.subscriber("order-created")
@billing_broker.publisher("invoice-requested")
async def handle_order(order_id: str, logger: Logger) -> str:
    logger.info("order %s created", order_id)
    return order_id
