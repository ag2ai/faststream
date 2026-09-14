import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress

import pytest
from aiokafka import AIOKafkaConsumer
from fast_depends.msgspec import MsgSpecSerializer
from schemas.msgspec import Schema

from faststream.kafka import KafkaBroker

from .test_basic import prefill_topic


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamKafkaMsgspecCase:
    comment = "Consume Msgspec Struct"
    broker_type = "Kafka"

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
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 0


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestPureKafkaMsgspecCase:
    comment = "Pure aio-kafka client with msgspec"
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
                    data = json.loads(msg.value.decode())
                    Schema(**data)
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
