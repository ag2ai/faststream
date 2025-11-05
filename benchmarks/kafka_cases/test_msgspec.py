import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from fast_depends.msgspec import MsgSpecSerializer
from faststream.kafka import KafkaBroker

from .schemas.msgspec import Schema


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=599,
    max_time=600,
)
class TestKafkaCase:
    comment = "Consume Msgspec Struct"
    broker_type = "Kafka"

    def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = KafkaBroker(
            logger=None,
            graceful_timeout=10,
            serializer=MsgSpecSerializer(use_fastdepends_errors=False),
        )

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
        async with self.start() as start_time:
            await asyncio.sleep(0.1)
        assert self.EVENTS_PROCESSED > 1
