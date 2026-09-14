import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import nats
import pytest

from faststream.nats import NatsBroker



async def prefill_stream(url: str, n: int) -> None:
    nc = await nats.connect(servers=[url])
    try:
        semaphore = asyncio.Semaphore(100)

        async def send() -> None:
            async with semaphore:
                message = {
                    "name": "John",
                    "age": 39,
                    "fullname": "LongString" * 8,
                    "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
                }
                await nc.publish("in", json.dumps(message).encode())

        await asyncio.gather(*(send() for _ in range(n)))
    finally:
        await nc.close()


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamNatsCase:
    comment = "Consume from JetStream"
    broker_type = "NATS"

    async def setup_method(self, prefill_messages: int) -> None:
        broker = self.broker = NatsBroker(logger=None, graceful_timeout=10)
        self.EVENTS_PROCESSED = 0
        self.PREFILL_MESSAGES = prefill_messages

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
            await prefill_stream("nats://localhost:4222", self.PREFILL_MESSAGES)

            yield start_time

    async def test_consume_message(self) -> None:
        async with self.start():
            while self.EVENTS_PROCESSED < self.PREFILL_MESSAGES:
                await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED == self.PREFILL_MESSAGES


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestPureNatsCase:
    comment = "Pure nats_py client"
    broker_type = "NATS"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0
        self.PREFILL_MESSAGES = prefill_messages

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        nc = await nats.connect(servers=["nats://localhost:4222"])

        async def message_handler(msg: Any) -> None:
            self.EVENTS_PROCESSED += 1

        sub = await nc.subscribe("in", cb=message_handler)
        start_time = time.time()
        await prefill_stream("nats://localhost:4222", self.PREFILL_MESSAGES)

        try:
            yield start_time
        finally:
            await sub.unsubscribe()
            await nc.close()

    async def test_consume_message(self) -> None:
        async with self.start():
            while self.EVENTS_PROCESSED < self.PREFILL_MESSAGES:
                await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED == self.PREFILL_MESSAGES
