import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from faststream._internal._compat import ExceptionGroup
from faststream._internal.logger.logger_proxy import EmptyLoggerObject
from faststream._internal.logger.state import LoggerState
from faststream.confluent import KafkaBroker
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

    def __len__(self) -> int:
        # Local queue length; the fake delivers everything inline, so it is
        # always empty by the time ``send_batch`` waits for a drain.
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
    """With the opt-in, batches are sent in ``queue.buffering.max.messages`` chunks."""

    class _CountingDrainProducer(AsyncConfluentProducer):
        drain_waits = 0

        async def _wait_for_queue_drain(self) -> None:
            _CountingDrainProducer.drain_waits += 1
            await super()._wait_for_queue_drain()

    fake = _FakeProducer()
    logger_state = LoggerState()
    logger_state.logger = EmptyLoggerObject()

    with patch(
        "faststream.confluent.helpers.client.Producer",
        return_value=fake,
    ):
        producer = _CountingDrainProducer(
            logger=logger_state,
            config=ConfluentFastConfig(config={"queue.buffering.max.messages": 2}),
        )

    try:
        batch = producer.create_batch()
        for i in range(5):
            batch.append(value=f"msg-{i}".encode())

        await producer.send_batch(
            batch,
            "topic",
            partition=None,
            retry_on_buffer_error=True,
        )

        assert len(fake.produced) == 5
        # 5 messages in chunks of 2 -> 3 chunks -> 2 in-between drain waits.
        assert _CountingDrainProducer.drain_waits == 2
    finally:
        await producer.stop()


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_send_batch_default_path_does_not_chunk() -> None:
    """Without the opt-in, ``send_batch`` behaves exactly like before: no chunking."""
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
        assert fake.flush_calls == 0
    finally:
        await producer.stop()


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_send_batch_queue_size_zero_means_no_chunking() -> None:
    """``queue.buffering.max.messages=0`` means "no limit" in librdkafka."""
    fake = _FakeProducer()
    producer = _make_producer(
        fake,
        ConfluentFastConfig(config={"queue.buffering.max.messages": 0}),
    )

    try:
        batch = producer.create_batch()
        for i in range(3):
            batch.append(value=f"msg-{i}".encode())

        # Must not raise ``ValueError: range() arg 3 must not be zero``.
        await producer.send_batch(
            batch,
            "topic",
            partition=None,
            retry_on_buffer_error=True,
        )

        assert len(fake.produced) == 3
        assert fake.flush_calls == 0
    finally:
        await producer.stop()


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_retry_with_message_timeout_zero_never_gives_up() -> None:
    """``message.timeout.ms=0`` means "no delivery timeout": retry until drained."""
    # Several consecutive BufferErrors, spanning more than one retry sleep.
    fake = _FakeProducer(fail_calls={1, 2, 3})
    producer = _make_producer(
        fake,
        ConfluentFastConfig(config={"message.timeout.ms": 0}),
    )

    try:
        batch = producer.create_batch()
        batch.append(value=b"msg")

        await producer.send_batch(
            batch,
            "topic",
            partition=None,
            retry_on_buffer_error=True,
        )

        assert len(fake.produced) == 1
        assert fake.produce_calls == 4
    finally:
        await producer.stop()


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_buffer_warning_logged_once_per_batch() -> None:
    """The BufferError warning fires once per ``send_batch``, not once per message."""
    # Every message overflows on its first produce call.
    fake = _FakeProducer(fail_calls={1, 2, 3})
    producer = _make_producer(fake)

    try:
        batch = producer.create_batch()
        for i in range(3):
            batch.append(value=f"msg-{i}".encode())

        with patch.object(
            producer, "logger_state", wraps=producer.logger_state
        ) as log_state:
            await producer.send_batch(
                batch,
                "topic",
                partition=None,
                retry_on_buffer_error=True,
            )

        assert log_state.log.call_count == 1
        assert len(fake.produced) == 3
    finally:
        await producer.stop()


@asynccontextmanager
async def _broker_with_fake_producer(
    fake: _FakeProducer,
) -> AsyncIterator[KafkaBroker]:
    """A connected ``KafkaBroker`` whose underlying confluent ``Producer`` is fake."""
    broker = KafkaBroker()

    with (
        patch(
            "faststream.confluent.helpers.client.Producer",
            return_value=fake,
        ),
        patch(
            "faststream.confluent.configs.broker.AdminService.connect",
            new=AsyncMock(),
        ),
    ):
        await broker.connect()
        try:
            yield broker
        finally:
            await broker.stop()


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_broker_publish_batch_retries_on_buffer_full() -> None:
    """The opt-in flag is reachable through ``broker.publish_batch(...)``."""
    fake = _FakeProducer(fail_calls={1})

    async with _broker_with_fake_producer(fake) as broker:
        await broker.publish_batch(
            "msg-0",
            "msg-1",
            "msg-2",
            topic="topic",
            retry_on_buffer_error=True,
        )

        assert len(fake.produced) == 3
        assert fake.produce_calls == 4


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_broker_publish_batch_fails_fast_by_default() -> None:
    """``broker.publish_batch(...)`` keeps its fail-fast default."""
    fake = _FakeProducer(fail_calls={1})

    async with _broker_with_fake_producer(fake) as broker:
        with pytest.raises(ExceptionGroup) as exc_info:
            await broker.publish_batch("msg-0", "msg-1", topic="topic")

        assert any(isinstance(exc, BufferError) for exc in exc_info.value.exceptions)


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_batch_publisher_publish_retries_on_buffer_full() -> None:
    """The opt-in flag is reachable through ``broker.publisher(batch=True).publish(...)``."""
    fake = _FakeProducer(fail_calls={1})

    async with _broker_with_fake_producer(fake) as broker:
        publisher = broker.publisher("topic", batch=True)

        await publisher.publish("msg-0", "msg-1", retry_on_buffer_error=True)

        assert len(fake.produced) == 2
        assert fake.produce_calls == 3


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_batch_publisher_publish_fails_fast_by_default() -> None:
    """``broker.publisher(batch=True).publish(...)`` keeps its fail-fast default."""
    fake = _FakeProducer(fail_calls={1})

    async with _broker_with_fake_producer(fake) as broker:
        publisher = broker.publisher("topic", batch=True)

        with pytest.raises(ExceptionGroup) as exc_info:
            await publisher.publish("msg-0", "msg-1")

        assert any(isinstance(exc, BufferError) for exc in exc_info.value.exceptions)


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
