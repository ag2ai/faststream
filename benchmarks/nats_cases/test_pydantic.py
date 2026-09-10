import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import nats
import pytest
from schemas.pydantic import Schema

from faststream.nats import NatsBroker


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamNatsPydanticCase:
    comment = "Consume Pydantic Model"
    broker_type = "NATS"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = NatsBroker(logger=None, graceful_timeout=10)

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
class TestPureNatsPydanticCase:
    comment = "Pure nats_py client with pydantic"
    broker_type = "NATS"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        nc = await nats.connect(servers=["nats://localhost:4222"])

        async def message_handler(msg: Any) -> None:
            self.EVENTS_PROCESSED += 1
            data = json.loads(msg.data.decode("utf-8"))
            parsed = Schema(**data)
            await nc.publish("in", parsed.model_dump_json().encode())

        await nc.subscribe("in", cb=message_handler)
        start_time = time.time()

        await nc.publish(
            "in",
            json.dumps({
                "name": "John",
                "age": 39,
                "fullname": "LongString" * 8,
                "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
            }).encode("utf-8"),
        )
        yield start_time

        await nc.close()

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 1
