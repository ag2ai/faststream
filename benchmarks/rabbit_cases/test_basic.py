import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import aio_pika
import pytest

from faststream.rabbit import RabbitBroker


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamRabbitCase:
    comment = "Consume Any Message"
    broker_type = "RabbitMQ"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

        broker = self.broker = RabbitBroker(logger=None, graceful_timeout=10)

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
class TestPureRabbitCase:
    comment = "Pure aio-pika client"
    broker_type = "RabbitMQ"

    async def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

    @asynccontextmanager
    async def start(self) -> AsyncGenerator[float, None]:
        connection = await aio_pika.connect_robust("amqp://guest:guest@localhost:5672/")
        channel = await connection.channel()

        async def handler(msg: aio_pika.IncomingMessage) -> None:
            async with msg.process():
                self.EVENTS_PROCESSED += 1
                data = json.loads(msg.body.decode())
                await channel.default_exchange.publish(
                    aio_pika.Message(json.dumps(data).encode()),
                    routing_key="in",
                )

        queue = await channel.declare_queue("in", durable=True)
        await queue.consume(handler)

        start_time = time.time()

        await channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps({
                    "name": "John",
                    "age": 39,
                    "fullname": "LongString" * 8,
                    "children": [
                        {"name": "Mike", "age": 8, "fullname": "LongString" * 8}
                    ],
                }).encode()
            ),
            routing_key="in",
        )

        yield start_time
        await connection.close()

    async def test_consume_message(self) -> None:
        async with self.start():
            await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED > 1
