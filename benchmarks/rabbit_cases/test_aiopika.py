import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aio_pika
import pytest

from .schemas.pydantic import Schema


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=599,
    max_time=600,
)
class TestRabbitCase:
    comment = "Pure aio-pika with pydantic"
    broker_type = "RabbitMQ"

    def setup_method(self) -> None:
        self.EVENTS_PROCESSED = 0

    @asynccontextmanager
    async def start(self) -> AsyncIterator[float]:
        connection = await aio_pika.connect_robust("amqp://guest:guest@localhost:5672/")
        channel = await connection.channel()

        async def handler(msg: aio_pika.IncomingMessage) -> None:
            self.EVENTS_PROCESSED += 1

            async with msg.process():
                data = json.loads(msg.body.decode())
                parsed = Schema(**data)

                await channel.default_exchange.publish(
                    aio_pika.Message(parsed.model_dump_json().encode()),
                    routing_key="in",
                )

        queue = await channel.declare_queue("in")
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
        async with self.start() as start_time:
            await asyncio.sleep(0.1)
        assert self.EVENTS_PROCESSED > 1
