import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import pytest
import redis.asyncio as redis

from faststream.redis import RedisBroker

PREFILL_MESSAGES = 200000


async def prefill_stream(url: str, n: int) -> None:
    client = redis.Redis.from_url(url, decode_responses=False)
    try:
        semaphore = asyncio.Semaphore(100)

        async def send() -> None:
            async with semaphore:
                await client.publish(
                    "in",
                    json.dumps({
                        "name": "John",
                        "age": 39,
                        "fullname": "LongString" * 8,
                        "children": json.dumps([
                            {"name": "Mike", "age": 8, "fullname": "LongString" * 8}
                        ]),
                    }),
                )

        await asyncio.gather(*(send() for _ in range(n)))
    finally:
        await client.aclose()


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamRedisCase:
    comment = "Consume Any Message"
    broker_type = "Redis"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = RedisBroker(logger=None, graceful_timeout=10)

        @broker.subscriber("in")
        async def handle(message: Any) -> Any:
            self.EVENTS_PROCESSED += 1
            return message

        self.handler = handle

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        async with self.broker:
            await self.broker.start()
            start_time = time.time()
            await prefill_stream("redis://localhost:6379", PREFILL_MESSAGES)
            yield start_time

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 1


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestPureRedisCase:
    comment = "Pure redis client"
    broker_type = "Redis"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0
        self.client = redis.Redis(host="localhost", port=6379, decode_responses=False)
        self.pubsub = self.client.pubsub()
        await self.pubsub.subscribe("in")
        await prefill_stream("redis://localhost:6379", PREFILL_MESSAGES)

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        print("Pripersya v start")
        start_time = time.time()
        async for msg in self.pubsub.listen():
            if msg["type"] != "message":
                continue
            self.EVENTS_PROCESSED += 1
            json.loads(msg["data"].decode())
            if self.EVENTS_PROCESSED >= 200000:
                break

        try:
            yield start_time
        finally:
            await self.pubsub.unsubscribe("in")
            await self.pubsub.aclose()
            await self.client.aclose()

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 1
