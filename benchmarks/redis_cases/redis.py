
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as redis

from .schemas.pydantic import Schema


class RedisTestCase:
    comment = "Pure redis client with pydantic"
    broker_type = "Redis"

    def __init__(self) -> None:
        self.EVENTS_PROCESSED = 0

    @asynccontextmanager
    async def start(self) -> AsyncIterator[float]:
        channel_name = "in"
        r = redis.Redis(host="localhost", port=6379, decode_responses=False)

        pubsub = r.pubsub()
        await pubsub.subscribe(channel_name)

        async def handler() -> None:
            self.EVENTS_PROCESSED += 1
            async for msg in pubsub.listen():
                decoded_msg = msg["data"]
                if decoded_msg == 1:
                    continue
                data = json.loads(msg["data"])
                validated = Schema(**data)
                await r.publish(
                    channel_name,
                    validated.model_dump_json()
                )

        start_time = time.time()

        await r.publish(
            channel_name,
            json.dumps({
                "name": "John",
                "age": 39,
                "fullname": "LongString" * 8,
                "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
            })
        )

        await handler()

        yield start_time

        await pubsub.unsubscribe(channel_name)
        await pubsub.close()
        await r.close()
