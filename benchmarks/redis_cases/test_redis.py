import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
import redis.asyncio as redis

from .schemas.pydantic import Schema


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=599,
    max_time=600,
)
class TestRedisCase:
    comment = "Pure redis client with pydantic"
    broker_type = "Redis"

    def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

    @asynccontextmanager
    async def start(self) -> AsyncIterator[float]:
        client = redis.Redis(host="localhost", port=6379, decode_responses=False)
        pubsub = client.pubsub()
        await pubsub.subscribe("in")

        async def handler() -> None:
            async for msg in pubsub.listen():
                decoded_msg = msg["data"]
                if decoded_msg == 1:
                    continue
                self.EVENTS_PROCESSED += 1
                data = json.loads(decoded_msg)
                validated = Schema(**data)
                await client.publish(
                    "in",
                    validated.model_dump_json()
                )

        start_time = time.time()

        await client.publish(
            "in",
            json.dumps({
                "name": "John",
                "age": 39,
                "fullname": "LongString" * 8,
                "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
            })
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
        async with self.start() as start_time:
            await asyncio.sleep(0.7)
        assert self.EVENTS_PROCESSED > 1
