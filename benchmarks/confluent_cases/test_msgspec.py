import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
from confluent_kafka import Consumer, TopicPartition
from fast_depends.msgspec import MsgSpecSerializer
from schemas.msgspec import Schema

from faststream._internal.utils.functions import run_in_executor
from faststream.confluent import KafkaBroker

from .test_basic import prefill_topic


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamConfluentMsgspecCase:
    comment = "Consume Msgspec Struct"
    broker_type = "Confluent"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = KafkaBroker(
            logger=None,
            graceful_timeout=10,
            serializer=MsgSpecSerializer(use_fastdepends_errors=False),
        )

        @broker.subscriber("in", auto_offset_reset="earliest")
        async def handle(message: Schema) -> Schema:
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
class TestPureConfluentMsgspecCase:
    comment = "Pure confluent client with msgspec"
    broker_type = "Confluent"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0

        self.consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
            "enable.auto.commit": True,
            "auto.offset.reset": "earliest",
        })
        self.consumer.assign([TopicPartition("in", 0, 0)])

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
                data = json.loads(msg.value().decode("utf-8"))
                Schema(**data)

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
