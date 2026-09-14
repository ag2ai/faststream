import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from typing import Any

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from faststream.kafka import KafkaBroker


async def prefill_topic(bootstrap_servers: str, n: int) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers="localhost:9092")
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(name="in", num_partitions=1, replication_factor=1)
        ])
    finally:
        await admin.close()
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        semaphore = asyncio.Semaphore(100)

        async def send(i: int) -> None:
            async with semaphore:
                message = (
                    {
                        "name": "John",
                        "age": 39,
                        "fullname": "LongString" * 8,
                        "children": [
                            {"name": "Mike", "age": 8, "fullname": "LongString" * 8}
                        ],
                    }
                    if i == 0
                    else {
                        "name": f"John-{i}",
                        "age": 39,
                        "fullname": "LongString" * 8,
                        "children": [
                            {"name": "Mike", "age": 8, "fullname": "LongString" * 8}
                        ],
                    }
                )
                await producer.send_and_wait("in", json.dumps(message).encode())

        await asyncio.gather(*(send(i) for i in range(n)))
    finally:
        await producer.stop()


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamKafkaCase:
    comment = "Consume Any Message"
    broker_type = "Kafka"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = KafkaBroker(logger=None, graceful_timeout=10)

        @broker.subscriber("in", auto_offset_reset="earliest")
        async def handle(message: Any) -> Any:
            self.EVENTS_PROCESSED += 1
            return message

        self.handler = handle

        await prefill_topic("localhost:9092", prefill_messages)

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        async with self.broker:
            await self.broker.start()
            start_time = time.time()

            yield start_time

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 0


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestPureKafkaCase:
    comment = "Pure aio-kafka client"
    broker_type = "Kafka"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0
        await prefill_topic("localhost:9092", prefill_messages)

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        consumer = AIOKafkaConsumer(
            "in",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await consumer.start()

        start_time = time.time()
        stop_event = asyncio.Event()

        async def message_loop() -> None:
            try:
                async for msg in consumer:
                    if stop_event.is_set():
                        break
                    self.EVENTS_PROCESSED += 1
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
            await consumer.stop()

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 0
