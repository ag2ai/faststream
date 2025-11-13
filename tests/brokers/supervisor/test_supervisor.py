import asyncio
import logging
from contextlib import suppress
from time import sleep

import pytest

from faststream._internal.endpoint.subscriber.supervisor import (
    TaskCallbackSupervisor,
    _SupervisorCache,
)


@pytest.mark.asyncio()
async def test_task_failing(subscriber_with_task_mixin):
    async def failing_task():
        raise ValueError

    logging.disable(logging.CRITICAL + 1)

    task = subscriber_with_task_mixin.add_task(failing_task)
    with suppress(ValueError):
        await task

    assert len(subscriber_with_task_mixin.tasks) > 1
    assert len(TaskCallbackSupervisor._TaskCallbackSupervisor__cache) == 1


@pytest.mark.asyncio()
async def test_task_successful(subscriber_with_task_mixin):
    async def successful_task():
        return True

    task = subscriber_with_task_mixin.add_task(successful_task)
    await task
    assert len(subscriber_with_task_mixin.tasks) == 1
    assert task.result()


@pytest.mark.asyncio()
async def test_ignore_cancellation_error(subscriber_with_task_mixin):
    async def cancelled_task():
        await asyncio.sleep(10)
        return True

    task = subscriber_with_task_mixin.add_task(cancelled_task)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(3)
    assert len(subscriber_with_task_mixin.tasks) == 1


def test_supervisor_cache(monkeypatch):
    monkeypatch.setenv("FASTSTREAM_SUPERVISOR_CACHE_TTL", "2")
    cache = _SupervisorCache()
    cache.add(1)

    assert 1 in cache
    sleep(2.5)
    assert 1 not in cache
    assert not len(cache)
