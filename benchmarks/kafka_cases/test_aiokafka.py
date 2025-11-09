import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from .schemas.pydantic import Schema


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestKafkaCase:
    comment = "Pure aio-kafka client with pydantic"
    broker_type = "Kafka"

    def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

    async def create_topic(self) -> None:
        admin = AIOKafkaAdminClient(bootstrap_servers="localhost:9092")
        await admin.start()
        try:
            await admin.create_topics([NewTopic(name="in", num_partitions=1, replication_factor=1)])
        finally:
            await admin.close()

    @asynccontextmanager
    async def start(self) -> AsyncIterator[float]:
        await self.create_topic()
        producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
        consumer = AIOKafkaConsumer(
            "in",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await producer.start()
        await consumer.start()

        start_time = time.time()
        stop_event = asyncio.Event()

        await producer.send_and_wait("in", json.dumps({
            "name": "John",
            "age": 39,
            "fullname": "LongString" * 8,
            "children": [
                {
                    "name": "Mike",
                    "age": 8,
                    "fullname": "LongString" * 8
                }
            ]
        }).encode())

        async def message_loop() -> None:
            try:
                async for msg in consumer:
                    if stop_event.is_set():
                        break
                    self.EVENTS_PROCESSED += 1
                    data = json.loads(msg.value.decode())
                    parsed = Schema(**data)
                    await producer.send_and_wait(
                        "in",
                        parsed.model_dump_json().encode()
                    )
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(message_loop())
        try:
            yield start_time
        finally:
            stop_event.set()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            await producer.stop()
            await consumer.stop()

    async def test_consume_message(self) -> None:
        async with self.start() as start_time:
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 1
