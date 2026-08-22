"""Kafka-specific `skip_none` behavior: tombstones.

In Kafka, a message with a key and a `None` (null) value is a *tombstone*:
during log compaction the broker deletes the record with that key.

By default a `None` handler return value is published as such a tombstone.
With `skip_none=True` the tombstone is never sent, so the key keeps its
last record.
"""

from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("delete-user")
@broker.publisher("user-events", key=b"user-1")
async def delete_user(msg: str, logger: Logger) -> str | None:
    logger.info("Deleting user: %s", msg)
    # keyed `None` is a tombstone: log compaction removes the "user-1" record
    return None


@broker.subscriber("archive-user")
@broker.publisher("user-events", key=b"user-2", skip_none=True)
async def archive_user(msg: str, logger: Logger) -> str | None:
    logger.info("Archiving user: %s", msg)
    # with `skip_none=True` no tombstone is sent,
    # so the "user-2" record survives log compaction
    return None


@broker.subscriber("user-events")
async def handle_event(msg: bytes, logger: Logger) -> None:
    logger.info("Event received: %r", msg)


@app.after_startup
async def test_publishing() -> None:
    await broker.publish("1", topic="delete-user")  # tombstone is published
    await broker.publish("2", topic="archive-user")  # nothing is published
