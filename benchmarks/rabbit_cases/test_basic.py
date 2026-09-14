import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import aio_pika
import pytest

from faststream.rabbit import RabbitBroker

QUEUE = "in"
RABBIT_URL = "amqp://guest:guest@localhost:5672/"


async def prefill_queue(url: str, n: int) -> None:
    connection = await aio_pika.connect_robust(url)
    try:
        channel = await connection.channel()
        await channel.declare_queue(QUEUE, durable=True)

        semaphore = asyncio.Semaphore(100)

        async def send(i: int) -> None:
            async with semaphore:
                message = {
                    "name": "John",
                    "age": 39,
                    "fullname": "LongString" * 8,
                    "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
                } if i == 0 else {
                    "name": f"John-{i}",
                    "age": 39,
                    "fullname": "LongString" * 8,
                    "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
                }
                await channel.default_exchange.publish(
                    aio_pika.Message(body=json.dumps(message).encode()),
                    routing_key=QUEUE,
                )

        await asyncio.gather(*(send(i) for i in range(n)))
    finally:
        await connection.close()


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamRabbitCase:
    comment = "Consume Any Message"
    broker_type = "RabbitMQ"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0
        self.PREFILL_MESSAGES = prefill_messages

        broker = self.broker = RabbitBroker(logger=None, graceful_timeout=10)

        @broker.subscriber(QUEUE)
        async def handle(message: Any) -> Any:
            self.EVENTS_PROCESSED += 1
            return message

        self.handler = handle

        await prefill_queue(RABBIT_URL, self.PREFILL_MESSAGES)

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        async with self.broker:
            await self.broker.start()
            start_time = time.time()

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
class TestPureRabbitCase:
    comment = "Pure aio-pika client"
    broker_type = "RabbitMQ"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0
        self.PREFILL_MESSAGES = prefill_messages
        await prefill_queue(RABBIT_URL, self.PREFILL_MESSAGES)

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        connection = await aio_pika.connect_robust(RABBIT_URL)
        channel = await connection.channel()

        async def handler(msg: aio_pika.IncomingMessage) -> None:
            async with msg.process():
                self.EVENTS_PROCESSED += 1

        queue = await channel.declare_queue(QUEUE, durable=True)
        await queue.consume(handler)

        start_time = time.time()

        try:
            yield start_time
        finally:
            await connection.close()

    async def test_consume_message(self) -> None:
        async with self.start():
            while self.EVENTS_PROCESSED < self.PREFILL_MESSAGES:
                await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED == self.PREFILL_MESSAGES
