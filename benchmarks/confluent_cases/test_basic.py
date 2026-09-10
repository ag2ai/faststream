import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from confluent_kafka import (
    Consumer,
    Producer,
    TopicPartition as CKTopicPartition,
)

from faststream._internal.utils.functions import run_in_executor
from faststream.confluent import KafkaBroker, TopicPartition


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamConfluentCase:
    comment = "Consume Any Message"
    broker_type = "Confluent"

    async def setup_method(self) -> None:
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
    async def start(self) -> AsyncGenerator[float, None]:
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
            await asyncio.sleep(6.0)
        assert self.EVENTS_PROCESSED > 1


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestPureConfluentCase:
    comment = "Pure confluent client"
    broker_type = "Confluent"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

        self.producer = Producer({
            "bootstrap.servers": "localhost:9092",
        })

        self.consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
            "enable.auto.commit": True,
            "auto.offset.reset": "earliest",
        })
        self.consumer.assign([CKTopicPartition("in", 0, 0)])

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        stop_event = asyncio.Event()

        def acked(err, msg) -> None:  # noqa: ANN001
            if err is not None:
                print(f"Failed to deliver message: {msg!s}: {err!s}")

        def handle() -> None:
            while not stop_event.is_set():
                try:
                    msg = self.consumer.poll(timeout=0.01)
                except RuntimeError:
                    break
                if msg is None:
                    continue
                self.EVENTS_PROCESSED += 1
                data = json.loads(msg.value().decode("utf-8"))
                self.producer.produce(
                    "in", value=json.dumps(data).encode("utf-8"), callback=acked
                )
                self.producer.flush()

        loop = asyncio.get_event_loop()
        start_time = time.time()
        executor_task = loop.run_in_executor(None, handle)

        value = json.dumps({
            "name": "John",
            "age": 39,
            "fullname": "LongString" * 8,
            "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
        }).encode("utf-8")

        await run_in_executor(None, self.producer.produce, "in", value=value)
        await run_in_executor(None, self.producer.poll, 0)

        try:
            yield start_time
        finally:
            stop_event.set()
            await executor_task
            await run_in_executor(None, self.producer.flush)
            await run_in_executor(None, self.consumer.close)

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(6.0)
        assert self.EVENTS_PROCESSED > 1
