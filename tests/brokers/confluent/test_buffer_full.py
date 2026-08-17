import asyncio
import time
from typing import Any
from unittest.mock import patch

import pytest

from faststream._internal._compat import ExceptionGroup
from faststream._internal.logger.logger_proxy import EmptyLoggerObject
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

    def __init__(self, fail_calls: set[int] | None = None) -> None:
        self._fail_calls = fail_calls or set()
        self.produce_calls = 0
        self.flush_calls = 0
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
        self.flush_calls += 1
        return 0


def _make_producer(
    fake: _FakeProducer,
    config: ConfluentFastConfig | None = None,
) -> AsyncConfluentProducer:
    logger_state = LoggerState()
    logger_state.logger = EmptyLoggerObject()

    with patch(
        "faststream.confluent.helpers.client.Producer",
        return_value=fake,
    ):
        return AsyncConfluentProducer(
            logger=logger_state,
            config=config or ConfluentFastConfig(),
        )


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_send_batch_survives_buffer_full() -> None:
    """A single ``BufferError`` must not cancel the whole batch.

    Regression test for #2836: a message over ``queue.buffering.max.messages``
    used to raise ``BufferError`` inside one ``send`` task, which cascaded
    through the ``send_batch`` task group and cancelled every sibling message.
    With ``retry_on_buffer_error=True`` the overflowing message should instead
    be retried after the queue drains.
    """
    # The first produce call hits a full queue; the retry must succeed.
    fake = _FakeProducer(fail_calls={1})
    producer = _make_producer(fake)

    try:
        batch = producer.create_batch()
        batch_size = 5
        for i in range(batch_size):
            batch.append(value=f"msg-{i}".encode())

        # Must not raise an ExceptionGroup / BufferError.
        await producer.send_batch(
            batch,
            "topic",
            partition=None,
            retry_on_buffer_error=True,
        )

        # Every message is enqueued, with exactly one extra produce call for
        # the retry that followed the BufferError.
        assert len(fake.produced) == batch_size
        assert fake.produce_calls == batch_size + 1
    finally:
        await producer.stop()


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_send_fails_fast_on_buffer_full() -> None:
    """Single ``send`` keeps its fail-fast semantics on a full queue."""
    fake = _FakeProducer(fail_calls={1})
    producer = _make_producer(fake)

    try:
        with pytest.raises(BufferError):
            await producer.send("topic", value=b"msg")
    finally:
        await producer.stop()


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_send_batch_fails_fast_by_default() -> None:
    """Without the opt-in, batch publishing surfaces ``BufferError`` immediately."""
    fake = _FakeProducer(fail_calls={1})
    producer = _make_producer(fake)

    try:
        batch = producer.create_batch()
        for i in range(3):
            batch.append(value=f"msg-{i}".encode())

        with pytest.raises(ExceptionGroup) as exc_info:
            await producer.send_batch(batch, "topic", partition=None)

        assert any(isinstance(exc, BufferError) for exc in exc_info.value.exceptions)
    finally:
        await producer.stop()


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_send_batch_chunks_by_queue_size() -> None:
    """Batches are sent in ``queue.buffering.max.messages`` chunks with a flush between."""
    fake = _FakeProducer()
    producer = _make_producer(
        fake,
        ConfluentFastConfig(config={"queue.buffering.max.messages": 2}),
    )

    try:
        batch = producer.create_batch()
        for i in range(5):
            batch.append(value=f"msg-{i}".encode())

        await producer.send_batch(batch, "topic", partition=None)

        assert len(fake.produced) == 5
        # 5 messages in chunks of 2 -> 3 chunks -> 2 in-between flushes.
        assert fake.flush_calls == 2
    finally:
        await producer.stop()


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_ack_callback_ignores_done_future() -> None:
    """A delivery callback for a cancelled future must not raise InvalidStateError.

    Second half of #2836: when one message in a batch fails, the task group
    cancels the sibling futures, but librdkafka still fires their delivery
    callbacks later.
    """

    class _DeferredAckProducer(_FakeProducer):
        """Collects delivery callbacks instead of firing them inline."""

        def __init__(self) -> None:
            super().__init__()
            self.callbacks: list[Any] = []

        def produce(
            self,
            topic: str,
            *,
            on_delivery: Any = None,
            **kwargs: Any,
        ) -> None:
            self.produce_calls += 1
            self.produced.append((topic, kwargs))
            self.callbacks.append(on_delivery)

    fake = _DeferredAckProducer()
    producer = _make_producer(fake)

    try:
        future = await producer.send("topic", value=b"msg", no_confirm=True)
        assert isinstance(future, asyncio.Future)
        future.cancel()

        # librdkafka delivers the report after the future was cancelled.
        fake.callbacks[0](None, None)
        await asyncio.sleep(0)  # let call_soon_threadsafe callbacks run

        assert future.cancelled()
    finally:
        await producer.stop()
