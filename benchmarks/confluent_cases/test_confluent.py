import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

from faststream._internal.utils.functions import run_in_executor

from .schemas.pydantic import Schema


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestConfluentCase:
    comment = "Pure confluent client with pydantic"
    broker_type = "Confluent"

    def setup_method(self) -> None:
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
        self.consumer.assign([TopicPartition("in", 0, 0)])

    @asynccontextmanager
    async def start(self) -> AsyncIterator[float]:
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
                parsed = Schema(**data)
                self.producer.produce(
                    "in", value=parsed.model_dump_json().encode("utf-8"), callback=acked
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
