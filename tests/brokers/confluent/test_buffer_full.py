import asyncio
import time
from collections.abc import AsyncIterator, Callable, Container, Iterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, patch

import anyio
import pytest

from faststream._internal._compat import ExceptionGroup
from faststream._internal.logger.logger_proxy import EmptyLoggerObject
from faststream._internal.logger.state import LoggerState
from faststream.confluent import KafkaBroker
from faststream.confluent.helpers.client import AsyncConfluentProducer, BatchBuilder
from faststream.confluent.helpers.config import ConfluentConfig, ConfluentFastConfig
from tests.tools import spy_decorator


class _FakeProducer:
    """`confluent_kafka.Producer` whose queue is full on the `full_on` produce calls."""

    def __init__(self, full_on: Container[int] = ()) -> None:
        self.full_on = full_on
        self.produce_calls = 0
        self.produced: list[bytes | None] = []

    def produce(
        self,
        topic: str,
        *,
        value: bytes | None = None,
        on_delivery: Callable[[Any, Any], None],
        **kwargs: Any,
    ) -> None:
        self.produce_calls += 1
        if self.produce_calls in self.full_on:
            msg = "Local: Queue full"
            raise BufferError(msg)
        self.produced.append(value)
        on_delivery(None, None)

    def poll(self, timeout: float = -1) -> int:
        # `_poll_loop` would spin on a poll that returns at once
        time.sleep(0.01)
        return 0

    def flush(self, timeout: float = -1) -> int:
        return 0

    def __len__(self) -> int:
        # deliveries are reported inline, so nothing is left in the local queue
        return 0


class _BusyQueueProducer(_FakeProducer):
    """Other publishers keep the shared local queue from ever draining."""

    def __len__(self) -> int:
        return 1


class _LateReportProducer(_FakeProducer):
    """Holds delivery reports back until the test fires them, as a later `poll()` does."""

    def __init__(self) -> None:
        super().__init__()
        self.reports: list[Callable[[Any, Any], None]] = []

    def produce(
        self,
        topic: str,
        *,
        value: bytes | None = None,
        on_delivery: Callable[[Any, Any], None],
        **kwargs: Any,
    ) -> None:
        self.reports.append(on_delivery)


@asynccontextmanager
async def _running_producer(
    fake: _FakeProducer,
    config: ConfluentConfig | None = None,
) -> AsyncIterator[AsyncConfluentProducer]:
    logger_state = LoggerState()
    logger_state.logger = EmptyLoggerObject()

    with patch("faststream.confluent.helpers.client.Producer", return_value=fake):
        producer = AsyncConfluentProducer(
            logger=logger_state,
            config=ConfluentFastConfig(config=config),
        )

    try:
        yield producer
    finally:
        await producer.stop()


@asynccontextmanager
async def _connected_broker(fake: _FakeProducer) -> AsyncIterator[KafkaBroker]:
    broker = KafkaBroker()

    with (
        patch("faststream.confluent.helpers.client.Producer", return_value=fake),
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


def _batch(producer: AsyncConfluentProducer, size: int) -> BatchBuilder:
    batch = producer.create_batch()
    for i in range(size):
        batch.append(value=f"msg-{i}".encode())
    return batch


@pytest.fixture()
def drain_waits() -> Iterator[AsyncMock]:
    spy = spy_decorator(AsyncConfluentProducer._wait_for_queue_drain)
    with patch.object(AsyncConfluentProducer, "_wait_for_queue_drain", spy):
        yield spy.mock


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_publish_batch_fails_fast_on_buffer_full() -> None:
    fake = _FakeProducer(full_on=(1,))

    async with _connected_broker(fake) as broker:
        with pytest.raises(ExceptionGroup) as exc_info:
            await broker.publish_batch("msg-0", "msg-1", topic="topic")

    assert [type(e) for e in exc_info.value.exceptions] == [BufferError]


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_publish_batch_retries_on_buffer_full() -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2836."""
    fake = _FakeProducer(full_on=(1,))

    async with _connected_broker(fake) as broker:
        await broker.publish_batch(
            "msg-0",
            "msg-1",
            "msg-2",
            topic="topic",
            retry_on_buffer_error=True,
        )

    # all three are delivered, the extra produce call is the retry
    assert (len(fake.produced), fake.produce_calls) == (3, 4)


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_batch_publisher_fails_fast_on_buffer_full() -> None:
    fake = _FakeProducer(full_on=(1,))

    async with _connected_broker(fake) as broker:
        publisher = broker.publisher("topic", batch=True)

        with pytest.raises(ExceptionGroup) as exc_info:
            await publisher.publish("msg-0", "msg-1")

    assert [type(e) for e in exc_info.value.exceptions] == [BufferError]


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_batch_publisher_retries_on_buffer_full() -> None:
    fake = _FakeProducer(full_on=(1,))

    async with _connected_broker(fake) as broker:
        publisher = broker.publisher("topic", batch=True)
        await publisher.publish("msg-0", "msg-1", retry_on_buffer_error=True)

    assert (len(fake.produced), fake.produce_calls) == (2, 3)


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_send_batch_chunks_by_queue_size(drain_waits: AsyncMock) -> None:
    fake = _FakeProducer()

    async with _running_producer(fake, {"queue.buffering.max.messages": 2}) as producer:
        await producer.send_batch(
            _batch(producer, 5),
            "topic",
            partition=None,
            retry_on_buffer_error=True,
        )

    # chunks of 2, 2 and 1, the queue drained before the second and the third
    assert (len(fake.produced), drain_waits.call_count) == (5, 2)


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_send_batch_does_not_chunk_by_default(drain_waits: AsyncMock) -> None:
    fake = _FakeProducer()

    async with _running_producer(fake, {"queue.buffering.max.messages": 2}) as producer:
        await producer.send_batch(_batch(producer, 5), "topic", partition=None)

    assert (len(fake.produced), drain_waits.call_count) == (5, 0)


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_send_batch_does_not_chunk_unlimited_queue(drain_waits: AsyncMock) -> None:
    fake = _FakeProducer()

    # `0` is librdkafka's "no limit", not a chunk size
    async with _running_producer(fake, {"queue.buffering.max.messages": 0}) as producer:
        await producer.send_batch(
            _batch(producer, 3),
            "topic",
            partition=None,
            retry_on_buffer_error=True,
        )

    assert (len(fake.produced), drain_waits.call_count) == (3, 0)


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_retry_never_gives_up_without_delivery_timeout() -> None:
    # full for longer than one retry sleep
    fake = _FakeProducer(full_on=(1, 2, 3))

    async with _running_producer(fake, {"delivery.timeout.ms": 0}) as producer:
        await producer.send_batch(
            _batch(producer, 1),
            "topic",
            partition=None,
            retry_on_buffer_error=True,
        )

    assert (len(fake.produced), fake.produce_calls) == (1, 4)


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_retry_gives_up_at_delivery_timeout() -> None:
    fake = _FakeProducer(full_on=(1, 2))

    async with _running_producer(fake, {"delivery.timeout.ms": 1}) as producer:
        with pytest.raises(ExceptionGroup) as exc_info:
            await producer.send_batch(
                _batch(producer, 1),
                "topic",
                partition=None,
                retry_on_buffer_error=True,
            )

    assert [type(e) for e in exc_info.value.exceptions] == [BufferError]


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_drain_wait_gives_up_at_delivery_timeout(drain_waits: AsyncMock) -> None:
    fake = _BusyQueueProducer()

    async with _running_producer(
        fake,
        {"queue.buffering.max.messages": 2, "delivery.timeout.ms": 1},
    ) as producer:
        with anyio.fail_after(3.0):
            await producer.send_batch(
                _batch(producer, 3),
                "topic",
                partition=None,
                retry_on_buffer_error=True,
            )

    # the second chunk went out although the queue never drained
    assert (len(fake.produced), drain_waits.call_count) == (3, 1)


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_buffer_full_logged_once_per_batch() -> None:
    # every message hits the full queue on its first produce call
    fake = _FakeProducer(full_on=(1, 2, 3))

    async with _running_producer(fake) as producer:
        with patch.object(
            producer,
            "logger_state",
            wraps=producer.logger_state,
        ) as logger_state:
            await producer.send_batch(
                _batch(producer, 3),
                "topic",
                partition=None,
                retry_on_buffer_error=True,
            )

    logger_state.log.assert_called_once()


@pytest.mark.confluent()
@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "report_before_cancel",
    (
        pytest.param(False, id="report after the cancel"),
        pytest.param(True, id="report already queued on the loop"),
    ),
)
async def test_late_delivery_report_is_ignored(report_before_cancel: bool) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2836."""
    fake = _LateReportProducer()
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _, context: loop_errors.append(context))

    try:
        async with _running_producer(fake) as producer:
            future = await producer.send("topic", value=b"msg", no_confirm=True)
            assert isinstance(future, asyncio.Future)

            # a sibling's failure cancels it around when librdkafka reports the delivery
            if report_before_cancel:
                fake.reports[0](None, None)
                future.cancel()
            else:
                future.cancel()
                fake.reports[0](None, None)
            await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(None)

    assert loop_errors == []
