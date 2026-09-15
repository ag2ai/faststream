import asyncio
import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import aio_pika
import pytest
from fast_depends.msgspec import MsgSpecSerializer
from schemas.msgspec import Schema

from faststream.rabbit import RabbitBroker

from .test_basic import QUEUE, RABBIT_URL, prefill_queue


@pytest.mark.asyncio()
@pytest.mark.benchmark(
    min_time=150,
    max_time=300,
)
class TestFaststreamRabbitMsgspecCase:
    comment = "Consume Msgspec Struct"
    broker_type = "RabbitMQ"

    async def setup_method(self, prefill_messages: int) -> None:
        self.EVENTS_PROCESSED = 0
        self.PREFILL_MESSAGES = prefill_messages

        broker = self.broker = RabbitBroker(
            logger=None,
            graceful_timeout=10,
            serializer=MsgSpecSerializer(use_fastdepends_errors=False),
        )

        @broker.subscriber("in")
        async def handle(message: Schema) -> Schema:
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
class TestPureRabbitMsgspecCase:
    comment = "Pure aio-pika client with msgspec"
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
                data = json.loads(msg.body.decode())
                Schema(**data)

        queue = await channel.declare_queue(QUEUE, durable=True)
        await queue.consume(handler)

        start_time = time.time()

        yield start_time
        await connection.close()

    async def test_consume_message(self) -> None:
        async with self.start():
            while self.EVENTS_PROCESSED < self.PREFILL_MESSAGES:
                await asyncio.sleep(1)
        assert self.EVENTS_PROCESSED == self.PREFILL_MESSAGES
