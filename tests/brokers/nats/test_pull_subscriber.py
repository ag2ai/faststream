import asyncio
from contextlib import suppress
from typing import Any

import pytest
from nats.errors import ConnectionClosedError, TimeoutError
from nats.js.errors import ServiceUnavailableError

from faststream.nats import JStream, NatsBroker, PullSub
from faststream.nats.subscriber.usecases import ConcurrentPullStreamSubscriber


class FakeSubscription:
    def __init__(self, messages: int = 2) -> None:
        self.messages = messages
        self.fetch_requests: list[int] = []
        self.next_fetch_started = asyncio.Event()

    async def fetch(self, *, batch: int, timeout: float | None) -> list[object]:
        self.fetch_requests.append(batch)

        if len(self.fetch_requests) <= self.messages:
            return [object()]

        self.next_fetch_started.set()
        await asyncio.Event().wait()
        return []


def create_subscriber(
    *, batch_size: int = 2, max_workers: int = 2
) -> ConcurrentPullStreamSubscriber:
    broker = NatsBroker()
    subscriber = broker.subscriber(
        "test",
        stream=JStream("test"),
        pull_sub=PullSub(batch_size=batch_size),
        max_workers=max_workers,
    )
    assert isinstance(subscriber, ConcurrentPullStreamSubscriber)
    return subscriber


async def stop_task(task: asyncio.Task[Any]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


@pytest.mark.asyncio()
@pytest.mark.nats()
@pytest.mark.parametrize(("batch_size", "max_workers"), ((5, 2), (1, 3)))
async def test_pull_waits_for_worker_capacity(batch_size: int, max_workers: int) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2563."""
    subscriber = create_subscriber(batch_size=batch_size, max_workers=max_workers)
    subscription: Any = FakeSubscription(messages=max_workers)
    subscriber.subscription = subscription
    subscriber.running = True

    handlers_started = 0
    all_handlers_started = asyncio.Event()
    release_handler = asyncio.Semaphore(0)

    async def consume(message: object) -> None:
        nonlocal handlers_started
        handlers_started += 1
        if handlers_started == max_workers:
            all_handlers_started.set()
        await release_handler.acquire()

    pull_task = asyncio.create_task(subscriber._consume_pull(cb=consume))

    try:
        await asyncio.wait_for(all_handlers_started.wait(), timeout=1)
        assert subscription.fetch_requests == [1] * max_workers

        release_handler.release()
        await asyncio.wait_for(subscription.next_fetch_started.wait(), timeout=1)
        assert subscription.fetch_requests == [1] * (max_workers + 1)

    finally:
        subscriber.running = False
        for _ in range(max_workers):
            release_handler.release()
        await stop_task(pull_task)

    assert subscriber.limiter.value == subscriber.max_workers


@pytest.mark.asyncio()
@pytest.mark.nats()
@pytest.mark.parametrize(
    "error", (TimeoutError, ConnectionClosedError, ServiceUnavailableError)
)
async def test_pull_releases_capacity_after_fetch_error(error: type[Exception]) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2563."""
    subscriber = create_subscriber()
    second_fetch_started = asyncio.Event()
    fetch_requests: list[int] = []

    class FailingOnceSubscription:
        async def fetch(
            self,
            *,
            batch: int,
            timeout: float | None,
        ) -> list[object]:
            fetch_requests.append(batch)
            if len(fetch_requests) == 1:
                raise error

            second_fetch_started.set()
            await asyncio.Event().wait()
            return []

    subscription: Any = FailingOnceSubscription()
    subscriber.subscription = subscription
    subscriber.running = True
    pull_task = asyncio.create_task(subscriber._consume_pull(cb=subscriber.consume))

    try:
        await asyncio.wait_for(second_fetch_started.wait(), timeout=1)
        assert fetch_requests == [1, 1]
    finally:
        subscriber.running = False
        await stop_task(pull_task)

    assert subscriber.limiter.value == subscriber.max_workers


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_pull_does_not_fetch_after_stop_while_waiting_for_capacity() -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2563."""
    subscriber = create_subscriber()
    subscription: Any = FakeSubscription()
    subscriber.subscription = subscription
    subscriber.running = True
    handlers_started = 0
    both_handlers_started = asyncio.Event()
    release_handlers = asyncio.Event()

    async def consume(message: object) -> None:
        nonlocal handlers_started
        handlers_started += 1
        if handlers_started == 2:
            both_handlers_started.set()
        await release_handlers.wait()

    pull_task = asyncio.create_task(subscriber._consume_pull(cb=consume))
    try:
        await asyncio.wait_for(both_handlers_started.wait(), timeout=1)
        subscriber.running = False
        release_handlers.set()
        await asyncio.wait_for(pull_task, timeout=1)
    finally:
        subscriber.running = False
        release_handlers.set()
        await stop_task(pull_task)

    assert subscription.fetch_requests == [1, 1]
    assert subscriber.limiter.value == subscriber.max_workers


@pytest.mark.asyncio()
@pytest.mark.nats()
@pytest.mark.parametrize("stop_before_release", (False, True))
async def test_unexpected_fetch_error_does_not_cancel_active_handler(
    stop_before_release: bool,
) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2563."""
    subscriber = create_subscriber()
    second_fetch_started = asyncio.Event()
    fetch_requests: list[int] = []

    class FailingSubscription:
        async def fetch(
            self,
            *,
            batch: int,
            timeout: float | None,
        ) -> list[object]:
            fetch_requests.append(batch)
            if len(fetch_requests) <= 2:
                return [object()]

            second_fetch_started.set()
            msg = "unexpected fetch error"
            raise RuntimeError(msg)

    subscription: Any = FailingSubscription()
    subscriber.subscription = subscription
    subscriber.running = True
    handlers_started = 0
    both_handlers_started = asyncio.Event()
    release_handler = asyncio.Semaphore(0)

    async def consume(message: object) -> None:
        nonlocal handlers_started
        handlers_started += 1
        if handlers_started == 2:
            both_handlers_started.set()
        await release_handler.acquire()

    pull_task = asyncio.create_task(subscriber._consume_pull(cb=consume))
    try:
        await asyncio.wait_for(both_handlers_started.wait(), timeout=1)

        release_handler.release()
        await asyncio.wait_for(second_fetch_started.wait(), timeout=1)
        await asyncio.sleep(0)
        assert not pull_task.done()

        if stop_before_release:
            subscriber.running = False

        release_handler.release()
        if stop_before_release:
            await pull_task
        else:
            with pytest.raises(RuntimeError, match="unexpected fetch error"):
                await pull_task
    finally:
        subscriber.running = False
        release_handler.release()
        release_handler.release()
        if not pull_task.done():
            await stop_task(pull_task)

    assert fetch_requests == [1, 1, 1]
    assert subscriber.limiter.value == subscriber.max_workers


@pytest.mark.asyncio()
@pytest.mark.nats()
@pytest.mark.parametrize("active_handlers", (0, 1))
async def test_pull_releases_capacity_when_fetch_is_cancelled(
    active_handlers: int,
) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2563."""
    subscriber = create_subscriber()
    fetch_started = asyncio.Event()
    release_handler = asyncio.Event()

    class BlockingSubscription:
        def __init__(self) -> None:
            self.requests = 0

        async def fetch(
            self,
            *,
            batch: int,
            timeout: float | None,
        ) -> list[object]:
            self.requests += 1
            if self.requests <= active_handlers:
                return [object()]
            fetch_started.set()
            await asyncio.Event().wait()
            return []

    subscription: Any = BlockingSubscription()
    subscriber.subscription = subscription
    subscriber.running = True

    async def consume(message: object) -> None:
        await release_handler.wait()

    pull_task = asyncio.create_task(subscriber._consume_pull(cb=consume))

    try:
        await asyncio.wait_for(fetch_started.wait(), timeout=1)
        assert subscriber.limiter.value == subscriber.max_workers - active_handlers - 1
    finally:
        subscriber.running = False
        await stop_task(pull_task)

    assert subscriber.limiter.value == subscriber.max_workers
