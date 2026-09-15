import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress

import pytest
from aiokafka import AIOKafkaConsumer
from schemas.pydantic import Schema

from faststream.kafka import KafkaBroker, KafkaMessage

from .test_basic import SequenceTrackingMixin, prefill_topic


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamKafkaPydanticCase(SequenceTrackingMixin):
    comment = "Consume Pydantic Model"
    broker_type = "Kafka"
    prefetch = None  
    batch = False
    ack_mode = "ack_first"  

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0
        self._init_sequence_tracking(prefill_messages)

        broker = self.broker = KafkaBroker(logger=None, graceful_timeout=10)

        @broker.subscriber("in", auto_offset_reset="earliest")
        async def handle(message: Schema, raw: KafkaMessage) -> Schema:
            self.EVENTS_PROCESSED += 1
            self._track_message(json.loads(raw.body.decode()))
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
class TestPureKafkaPydanticCase(SequenceTrackingMixin):
    comment = "Pure aio-kafka client with pydantic"
    broker_type = "Kafka"
    prefetch = None  # max_poll_records not overridden, aiokafka default
    batch = False
    ack_mode = "auto_commit"  # enable_auto_commit=True below

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0
        self._init_sequence_tracking(prefill_messages)
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
                    self._track_message(data)
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
