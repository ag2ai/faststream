import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from typing import Any

import pytest
from confluent_kafka import (
    Consumer,
    Producer,
    TopicPartition as CKTopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic

from faststream._internal.utils.functions import run_in_executor
from faststream.confluent import KafkaBroker, TopicPartition


async def prefill_topic(bootstrap_servers: str, n: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.create_topics([
        NewTopic("in", num_partitions=1, replication_factor=1)
    ])
    for future in futures.values():
        with suppress(Exception):
            await run_in_executor(None, future.result)

    producer = Producer({"bootstrap.servers": bootstrap_servers})

    for i in range(n):
        message = {
            "name": "John",
            "age": 39,
            "fullname": "LongString" * 8,
            "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
        } if i == 0 else {
            "name": f"John-{i}",
            "age": 39,
            "fullname": "LongString" * 8,
            "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
        }
        producer.produce("in", value=json.dumps(message).encode("utf-8"))
        if i % 1000 == 0:
            producer.poll(0)

    await run_in_executor(None, producer.flush)


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamConfluentCase:
    comment = "Consume Any Message"
    broker_type = "Confluent"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = KafkaBroker(logger=None, graceful_timeout=10)

        @broker.subscriber(
            partitions=[TopicPartition("in", 0)], auto_offset_reset="earliest"
        )
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
            await asyncio.sleep(6.0)
        assert self.EVENTS_PROCESSED > 0


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestPureConfluentCase:
    comment = "Pure confluent client"
    broker_type = "Confluent"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0

        self.consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
            "enable.auto.commit": True,
            "auto.offset.reset": "earliest",
        })
        self.consumer.assign([CKTopicPartition("in", 0, 0)])

        await prefill_topic("localhost:9092", prefill_messages)

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        stop_event = asyncio.Event()

        def handle() -> None:
            while not stop_event.is_set():
                try:
                    msg = self.consumer.poll(timeout=0.01)
                except RuntimeError:
                    break
                if msg is None:
                    continue
                self.EVENTS_PROCESSED += 1
                json.loads(msg.value().decode("utf-8"))

        loop = asyncio.get_event_loop()
        start_time = time.time()
        executor_task = loop.run_in_executor(None, handle)

        try:
            yield start_time
        finally:
            stop_event.set()
            await executor_task
            await run_in_executor(None, self.consumer.close)

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(6.0)
        assert self.EVENTS_PROCESSED > 0
