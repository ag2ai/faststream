import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from typing import Any

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import UnknownTopicOrPartitionError

from faststream.kafka import KafkaBroker


class SequenceTrackingMixin:

    def _init_sequence_tracking(self, expected_total: int) -> None:
        self._expected_total = expected_total
        self._seen_seqs: set[int] = set()
        self.rtt_samples_ms: list[float] = []
        self.duplicated = 0

    def _track_message(self, data: dict) -> None:
        seq = data.get("_seq")
        if seq is not None:
            if seq in self._seen_seqs:
                self.duplicated += 1
            else:
                self._seen_seqs.add(seq)

        pub_ts = data.get("_pub_ts")
        if pub_ts is not None:
            self.rtt_samples_ms.append((time.perf_counter() - pub_ts) * 1000)

    @property
    def dropped(self) -> int:
        return max(self._expected_total - len(self._seen_seqs), 0)


async def prefill_topic(bootstrap_servers: str, n: int) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers="localhost:9092")
    await admin.start()
    try:
        await admin.delete_topics(["in"])
        await admin.create_topics([
            NewTopic(name="in", num_partitions=1, replication_factor=1)
        ])
    except UnknownTopicOrPartitionError:
        print("Топик 'in' не существует")
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
                message["_seq"] = i
                message["_pub_ts"] = time.perf_counter()
                await producer.send_and_wait("in", json.dumps(message).encode())

        await asyncio.gather(*(send(i) for i in range(n)))
    finally:
        await producer.stop()


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamKafkaCase(SequenceTrackingMixin):
    comment = "Consume Any Message"
    broker_type = "Kafka"
    prefetch = None
    batch = False
    ack_mode = "ack_first"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0
        self._init_sequence_tracking(prefill_messages)

        broker = self.broker = KafkaBroker(logger=None, graceful_timeout=10)

        @broker.subscriber("in", auto_offset_reset="earliest")
        async def handle(message: Any) -> Any:
            self.EVENTS_PROCESSED += 1
            self._track_message(message)
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
class TestPureKafkaCase(SequenceTrackingMixin):
    comment = "Pure aio-kafka client"
    broker_type = "Kafka"
    prefetch = None
    batch = False
    ack_mode = "auto_commit"

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
                    self._track_message(json.loads(msg.value.decode()))
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
