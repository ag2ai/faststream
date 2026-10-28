import logging

from aio_pika.abc import AbstractConnection, AbstractRobustConnection

from faststream import FastStream, Logger
from faststream.rabbit import RabbitBroker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rabbit.events")


async def on_close(
    connection: AbstractConnection | None,
    error: BaseException | None,
) -> None:
    if connection is not None and connection.close_called:
        logger.info("RabbitMQ connection closed during shutdown")
    else:
        logger.warning("RabbitMQ connection closed: %r", error)


async def on_reconnected(connection: AbstractRobustConnection | None) -> None:
    logger.info("RabbitMQ connection restored")


broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",
    on_close=[on_close],
    on_reconnected=[on_reconnected],
)
app = FastStream(broker)


@broker.subscriber("events.example")
async def handle(message: str, logger: Logger) -> None:
    logger.info("Received: %s", message)
