import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from faststream.nats import NatsBroker

from .schemas.pydantic import Schema


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestNatsTestCase:
    comment = "Consume Pydantic Model"
    broker_type = "NATS"

    def setup_method(self) -> None:
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

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 1
