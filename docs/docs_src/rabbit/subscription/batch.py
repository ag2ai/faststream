import asyncio
from faststream import AckPolicy, FastStream, Logger
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage, RabbitQueue

broker = RabbitBroker()
app = FastStream(broker)
queue = RabbitQueue("example_queue")
BATCH_SIZE = 10
lock = asyncio.Lock()
batch: list[tuple[dict, RabbitMessage]] = []
processed: list[list[dict]] = []  # used in docs tests


def take_batch() -> list[tuple[dict, RabbitMessage]]:
    """Detach the current batch while the lock is held."""
    items = batch.copy()
    batch.clear()
    return items


async def flush(items: list[tuple[dict, RabbitMessage]], logger: Logger) -> None:
    payloads = [msg for msg, _ in items]
    processed.append(payloads)
    logger.info("Processing batch of %s messages", len(payloads))
    # your batch work here
    for _, raw in items:
        await raw.ack()


@broker.subscriber(
    queue,
    channel=Channel(prefetch_count=BATCH_SIZE * 2),
    ack_policy=AckPolicy.MANUAL,
)
async def handle(message: dict, raw_message: RabbitMessage, logger: Logger) -> None:
    items: list[tuple[dict, RabbitMessage]] | None = None
    async with lock:
        batch.append((message, raw_message))
        if len(batch) >= BATCH_SIZE:
            items = take_batch()  # detach under the lock
    if items is not None:
        await flush(items, logger)  # process outside the lock
