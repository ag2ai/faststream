import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aio_pika


class RabbitTestCase:
    comment = "Pure aio-pika"
    broker_type = "RabbitMQ"

    def __init__(self) -> None:
        self.EVENTS_PROCESSED = 0

    @asynccontextmanager
    async def start(self) -> AsyncIterator[float]:
        connection = await aio_pika.connect_robust("amqp://guest:guest@localhost:5672/")
        channel = await connection.channel()

        async def handler(msg: aio_pika.IncomingMessage) -> None:
            self.EVENTS_PROCESSED += 1

            async with msg.process():
                await channel.default_exchange.publish(
                    aio_pika.Message(msg.body),
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
