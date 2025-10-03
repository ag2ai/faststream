import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from faststream.confluent import KafkaBroker, TopicPartition


class ConfluentTestCase:
    comment = "Consume Any Message"
    broker_type = "Confluent"

    def __init__(self) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = KafkaBroker(logger=None, graceful_timeout=10)

        p = self.publisher = broker.publisher("in")

        @p
        @broker.subscriber(
            partitions=[TopicPartition("in", 0)], auto_offset_reset="earliest"
        )
        async def handle(message: Any) -> Any:
            self.EVENTS_PROCESSED += 1
            return message

        self.handler = handle

    @asynccontextmanager
    async def start(self) -> AsyncIterator[float]:
        async with self.broker:
            await self.broker.start()
            start_time = time.time()

            await self.publisher.publish({
                "name": "John",
                "age": 39,
                "fullname": "LongString" * 8,
                "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
            })

            yield start_time
