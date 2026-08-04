import time
from typing import Any
from unittest.mock import patch

import pytest

from faststream._internal.logger.state import LoggerState
from faststream.confluent.helpers.client import AsyncConfluentProducer
from faststream.confluent.helpers.config import ConfluentFastConfig


class _FakeProducer:
    """Stand-in for ``confluent_kafka.Producer``.

    ``produce`` raises ``BufferError`` ("Local: Queue full") on the call
    indices listed in ``fail_calls`` (mimicking librdkafka rejecting a message
    once its local queue is full), and otherwise immediately fires the delivery
    callback so the awaiting future resolves.
    """

    def __init__(self, fail_calls: set[int]) -> None:
        self._fail_calls = fail_calls
        self.produce_calls = 0
        self.produced: list[tuple[str, dict[str, Any]]] = []

    def produce(self, topic: str, *, on_delivery: Any = None, **kwargs: Any) -> None:
        self.produce_calls += 1
        if self.produce_calls in self._fail_calls:
            msg = "Local: Queue full"
            raise BufferError(msg)
        self.produced.append((topic, kwargs))
        if on_delivery is not None:
            on_delivery(None, None)

    def poll(self, timeout: float = 0) -> int:
        # Throttle the background poll loop a little so it does not busy-spin.
        time.sleep(0.01)
        return 0

    def flush(self, *args: Any, **kwargs: Any) -> int:
        return 0


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_send_batch_survives_buffer_full() -> None:
    """A single ``BufferError`` must not cancel the whole batch.

    Regression test for #2836: a message over ``queue.buffering.max.messages``
    used to raise ``BufferError`` inside one ``send`` task, which cascaded
    through the ``send_batch`` task group and cancelled every sibling message.
    The overflowing message should instead be retried after draining the queue.
    """
    # The first produce call hits a full queue; the retry must succeed.
    fake = _FakeProducer(fail_calls={1})

    with patch(
        "faststream.confluent.helpers.client.Producer",
        return_value=fake,
    ):
        producer = AsyncConfluentProducer(
            logger=LoggerState(),
            config=ConfluentFastConfig(),
        )

    try:
        batch = producer.create_batch()
        batch_size = 5
        for i in range(batch_size):
            batch.append(value=f"msg-{i}".encode())

        # Must not raise an ExceptionGroup / BufferError.
        await producer.send_batch(batch, "topic", partition=None)

        # Every message is enqueued, with exactly one extra produce call for the
        # retry that followed the BufferError.
        assert len(fake.produced) == batch_size
        assert fake.produce_calls == batch_size + 1
    finally:
        await producer.stop()
