import asyncio
import threading
from typing import TYPE_CHECKING, Any

import pytest

from faststream.confluent.helpers.client import AsyncConfluentProducer, BatchBuilder

if TYPE_CHECKING:
    from collections.abc import Callable


class _DeliveredMessage:
    def error(self) -> None:
        return None


class _BoundedProducer:
    """Mimics librdkafka's local queue: `produce` raises `BufferError` when full."""

    def __init__(self, max_messages: int) -> None:
        self.max_messages = max_messages
        self.queue: list[tuple[bytes, Callable[[Any, Any], None]]] = []
        self.delivered: list[bytes] = []
        self.lock = threading.Lock()

    def produce(self, topic: str, *, value: bytes, on_delivery: Any, **_: Any) -> None:
        with self.lock:
            if len(self.queue) >= self.max_messages:
                msg = "Local: Queue full"
                raise BufferError(msg)
            self.queue.append((value, on_delivery))

    def poll(self, timeout: float) -> int:
        with self.lock:
            served, self.queue = self.queue, []
        for value, on_delivery in served:
            self.delivered.append(value)
            on_delivery(None, _DeliveredMessage())
        return len(served)


@pytest.mark.confluent()
@pytest.mark.asyncio()
@pytest.mark.parametrize("no_confirm", (False, True))
async def test_send_batch_waits_for_space_when_local_queue_is_full(
    no_confirm: bool,
) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2836."""
    driver = _BoundedProducer(max_messages=2)
    producer = AsyncConfluentProducer.__new__(AsyncConfluentProducer)
    producer.producer = driver

    batch = BatchBuilder()
    values = [f"msg-{i}".encode() for i in range(5)]
    for value in values:
        batch.append(value=value)

    async def poll_loop() -> None:  # stands in for the producer's background poll task
        while True:
            await asyncio.sleep(0.01)
            driver.poll(0)

    poller = asyncio.create_task(poll_loop())
    try:
        await asyncio.wait_for(
            producer.send_batch(batch, "topic", partition=None, no_confirm=no_confirm),
            timeout=5,
        )
    finally:
        poller.cancel()
    driver.poll(0)

    assert sorted(driver.delivered) == values
