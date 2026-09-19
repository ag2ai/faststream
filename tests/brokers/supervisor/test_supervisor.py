import asyncio
import logging
from contextlib import suppress
from typing import Any
from unittest.mock import patch

import pytest

from faststream._internal.endpoint.subscriber.supervisor import (
    TaskCallbackSupervisor,
    _SupervisorCache,
)


@pytest.mark.asyncio()
async def test_task_failing(subscriber_with_task_mixin: Any) -> None:
    async def failing_task() -> Any:
        raise ValueError

    logging.disable(logging.CRITICAL + 1)

    task = subscriber_with_task_mixin.add_task(failing_task)
    with suppress(ValueError):
        await task

    assert len(subscriber_with_task_mixin.tasks) > 1
    assert len(TaskCallbackSupervisor._TaskCallbackSupervisor__cache) == 1


@pytest.mark.asyncio()
async def test_task_failing_without_restart(subscriber_with_task_mixin: Any) -> None:
    async def failing_task() -> Any:
        raise ValueError

    task = subscriber_with_task_mixin.add_task(
        failing_task,
        restart_on_failure=False,
    )
    with suppress(ValueError):
        await task
    await asyncio.sleep(0)

    assert len(subscriber_with_task_mixin.tasks) == 1


@pytest.mark.asyncio()
async def test_task_successful(subscriber_with_task_mixin: Any) -> Any:
    async def successful_task() -> Any:
        return True

    task = subscriber_with_task_mixin.add_task(successful_task)
    await task
    assert len(subscriber_with_task_mixin.tasks) == 1
    assert task.result()


@pytest.mark.asyncio()
@pytest.mark.slow()
async def test_ignore_cancellation_error(subscriber_with_task_mixin: Any) -> Any:
    async def cancelled_task() -> Any:
        await asyncio.sleep(10)
        return True

    task = subscriber_with_task_mixin.add_task(cancelled_task)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(3)
    assert len(subscriber_with_task_mixin.tasks) == 1


def test_supervisor_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    with patch("time.time") as mocked_time:
        mocked_time.return_value = 0
        cache = _SupervisorCache()
        cache.add(1)
        cache.add(2)
        mocked_time.return_value = cache.ttl - 1
        assert 1 in cache
        assert 2 in cache
        assert 1 in cache
        mocked_time.return_value = cache.ttl + 1
        assert 1 not in cache
        assert 2 not in cache
