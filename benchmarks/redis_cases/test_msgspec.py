import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
import redis.asyncio as redis
from fast_depends.msgspec import MsgSpecSerializer
from schemas.msgspec import Schema

from faststream.redis import RedisBroker


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamRedisMsgspecCase:
    comment = "Consume Msgspec Struct"
    broker_type = "Redis"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = RedisBroker(
            logger=None,
            graceful_timeout=10,
            serializer=MsgSpecSerializer(use_fastdepends_errors=False),
        )

        p = self.publisher = broker.publisher("in")

        @p
        @broker.subscriber("in")
        async def handle(message: Schema) -> Schema:
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
class TestPureRedisMsgspecCase:
    comment = "Pure redis client with pydantic"
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
                validated = Schema(**data)
                await client.publish("in", validated.to_json())

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
