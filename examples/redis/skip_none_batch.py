"""Redis-specific `skip_none` behavior: batch list publishers.

Redis list publishers with `batch=True` publish a batch of values at once.
With `skip_none=True`, `None` values are excluded from the batch, and a
batch that consists of `None` values only is skipped entirely.
"""

from faststream import FastStream, Logger
from faststream.redis import ListSub, RedisBroker

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(channel="commands")
@broker.publisher(list=ListSub("user-list", batch=True), skip_none=True)
async def handle(msg: str, logger: Logger) -> list[str | None]:
    logger.info(msg)
    if msg == "empty":
        # every batch value is `None` -> the whole batch is skipped
        return [None, None]

    # `None` values are excluded from the batch
    return ["Hi!", None, msg]


@broker.subscriber(list=ListSub("user-list", batch=True))
async def handle_list(msg: list[str], logger: Logger) -> None:
    logger.info("List batch: %s", msg)


@app.after_startup
async def test_publishing() -> None:
    # -> batch ["Hi!", "FastStream"] is published
    await broker.publish("FastStream", channel="commands")

    # -> nothing is published
    await broker.publish("empty", channel="commands")
