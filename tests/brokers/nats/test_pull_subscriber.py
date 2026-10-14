import asyncio
from contextlib import suppress
from typing import Any

import pytest
from nats.errors import TimeoutError

from faststream.nats import JStream, NatsBroker, PullSub
from faststream.nats.subscriber.usecases import ConcurrentPullStreamSubscriber


class FakeSubscription:
    def __init__(self) -> None:
        self.fetch_requests: list[int] = []
        self.second_fetch_started = asyncio.Event()

    async def fetch(self, *, batch: int, timeout: float | None) -> list[object]:
        self.fetch_requests.append(batch)

        if len(self.fetch_requests) == 1:
            return [object() for _ in range(batch)]

        self.second_fetch_started.set()
        await asyncio.Event().wait()
        return []


def create_subscriber(*, batch_size: int = 2) -> ConcurrentPullStreamSubscriber:
    broker = NatsBroker()
    subscriber = broker.subscriber(
        "test",
        stream=JStream("test"),
        pull_sub=PullSub(batch_size=batch_size, batch=True),
        max_workers=2,
    )
    assert isinstance(subscriber, ConcurrentPullStreamSubscriber)
    return subscriber


async def stop_task(task: asyncio.Task[Any]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_pull_waits_for_worker_capacity() -> None:
    subscriber = create_subscriber(batch_size=5)
    subscription = FakeSubscription()
    subscriber.subscription = subscription  # type: ignore[assignment]
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
        for _ in range(20):
            await asyncio.sleep(0)

        assert subscription.fetch_requests == [2]
        assert not subscription.second_fetch_started.is_set()

        release_handler.release()
        await asyncio.wait_for(subscription.second_fetch_started.wait(), timeout=1)
        assert subscription.fetch_requests == [2, 1]

    finally:
        subscriber.running = False
        release_handler.release()
        release_handler.release()
        await stop_task(pull_task)

    assert subscriber.limiter.value == subscriber.max_workers


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_pull_releases_unused_fetch_capacity() -> None:
    subscriber = create_subscriber()
    second_fetch_started = asyncio.Event()
    fetch_requests: list[int] = []

    class PartialSubscription:
        async def fetch(
            self,
            *,
            batch: int,
            timeout: float | None,
        ) -> list[object]:
            fetch_requests.append(batch)
            if len(fetch_requests) == 1:
                return [object()]

            second_fetch_started.set()
            await asyncio.Event().wait()
            return []

    subscriber.subscription = PartialSubscription()  # type: ignore[assignment]
    subscriber.running = True
    release_handler = asyncio.Event()

    async def consume(message: object) -> None:
        await release_handler.wait()

    pull_task = asyncio.create_task(subscriber._consume_pull(cb=consume))

    try:
        await asyncio.wait_for(second_fetch_started.wait(), timeout=1)
        assert fetch_requests == [2, 1]

    finally:
        subscriber.running = False
        release_handler.set()
        await stop_task(pull_task)

    assert subscriber.limiter.value == subscriber.max_workers


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_pull_releases_capacity_after_timeout() -> None:
    subscriber = create_subscriber()
    second_fetch_started = asyncio.Event()
    fetch_requests: list[int] = []

    class TimingOutSubscription:
        async def fetch(
            self,
            *,
            batch: int,
            timeout: float | None,
        ) -> list[object]:
            fetch_requests.append(batch)
            if len(fetch_requests) == 1:
                raise TimeoutError

            second_fetch_started.set()
            await asyncio.Event().wait()
            return []

    subscriber.subscription = TimingOutSubscription()  # type: ignore[assignment]
    subscriber.running = True
    pull_task = asyncio.create_task(subscriber._consume_pull(cb=subscriber.consume))

    await asyncio.wait_for(second_fetch_started.wait(), timeout=1)
    assert fetch_requests == [2, 2]

    subscriber.running = False
    await stop_task(pull_task)

    assert subscriber.limiter.value == subscriber.max_workers


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_pull_does_not_fetch_after_stop_while_waiting_for_capacity() -> None:
    subscriber = create_subscriber()
    subscription = FakeSubscription()
    subscriber.subscription = subscription  # type: ignore[assignment]
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
    await asyncio.wait_for(both_handlers_started.wait(), timeout=1)

    subscriber.running = False
    release_handlers.set()
    await asyncio.wait_for(pull_task, timeout=1)

    assert subscription.fetch_requests == [2]
    assert subscriber.limiter.value == subscriber.max_workers


@pytest.mark.asyncio()
@pytest.mark.nats()
@pytest.mark.parametrize("stop_before_release", (False, True))
async def test_unexpected_fetch_error_does_not_cancel_active_handler(
    stop_before_release: bool,
) -> None:
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
            if len(fetch_requests) == 1:
                return [object(), object()]

            second_fetch_started.set()
            msg = "unexpected fetch error"
            raise RuntimeError(msg)

    subscriber.subscription = FailingSubscription()  # type: ignore[assignment]
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

    assert fetch_requests == [2, 1]
    assert subscriber.limiter.value == subscriber.max_workers


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_pull_releases_capacity_when_fetch_is_cancelled() -> None:
    subscriber = create_subscriber()
    fetch_started = asyncio.Event()

    class BlockingSubscription:
        async def fetch(
            self,
            *,
            batch: int,
            timeout: float | None,
        ) -> list[object]:
            fetch_started.set()
            await asyncio.Event().wait()
            return []

    subscriber.subscription = BlockingSubscription()  # type: ignore[assignment]
    subscriber.running = True
    pull_task = asyncio.create_task(subscriber._consume_pull(cb=subscriber.consume))

    await asyncio.wait_for(fetch_started.wait(), timeout=1)
    assert subscriber.limiter.value == 0

    subscriber.running = False
    await stop_task(pull_task)

    assert subscriber.limiter.value == subscriber.max_workers
