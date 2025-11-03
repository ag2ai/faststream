import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from faststream.nats import NatsBroker

from .schemas.pydantic import Schema


class NatsTestCase:
    comment = "Consume Pydantic Model"
    broker_type = "NATS"

    def __init__(self) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = NatsBroker(logger=None, graceful_timeout=10)

        p = self.publisher = broker.publisher("in")

        @p
        @broker.subscriber("in")
        async def handle(message: Schema) -> Schema:
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
