import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import pytest
import redis.asyncio as redis

from faststream.redis import RedisBroker


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

        p = self.publisher = broker.publisher("in")

        @p
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

            await self.publisher.publish({
                "name": "John",
                "age": 39,
                "fullname": "LongString" * 8,
                "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
            })

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

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        client = redis.Redis(host="localhost", port=6379, decode_responses=False)
        pubsub = client.pubsub()
        await pubsub.subscribe("in")

        async def handler() -> None:
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                self.EVENTS_PROCESSED += 1
                data = json.loads(msg["data"].decode())
                await client.publish("in", json.dumps(data))

        start_time = time.time()

        await client.publish(
            "in",
            json.dumps({
                "name": "John",
                "age": 39,
                "fullname": "LongString" * 8,
                "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
            }),
        )

        handler_task = asyncio.create_task(handler())

        try:
            yield start_time
        finally:
            handler_task.cancel()
            await pubsub.unsubscribe("in")
            await pubsub.aclose()
            await client.aclose()

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 1
