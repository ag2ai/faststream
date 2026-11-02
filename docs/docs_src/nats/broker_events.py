import logging

from faststream import FastStream, Logger
from faststream.nats import NatsBroker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nats.events")


async def on_error(error: Exception) -> None:
    logger.error("NATS client error: %s", error)


async def on_disconnected() -> None:
    logger.warning("Disconnected from NATS")


async def on_closed() -> None:
    logger.info("NATS connection closed. Automatic reconnection has stopped")


async def on_discovered_server() -> None:
    logger.info("Discovered new NATS servers")


async def on_reconnected() -> None:
    logger.info("Reconnected to NATS")


broker = NatsBroker(
    "nats://localhost:4222",
    error_cb=on_error,
    disconnected_cb=on_disconnected,
    closed_cb=on_closed,
    discovered_server_cb=on_discovered_server,
    reconnected_cb=on_reconnected,
)
app = FastStream(broker)


@broker.subscriber("events.example")
async def handle(message: str, logger: Logger) -> None:
    logger.info("Received: %s", message)
