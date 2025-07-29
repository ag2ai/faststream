import logging
from contextlib import suppress

import pytest


@pytest.mark.asyncio
async def test_task_failing(subscriber_with_task_mixin, caplog):
    async def failing_task():
        raise ValueError

    logging.disable(logging.CRITICAL + 1)

    task = subscriber_with_task_mixin.add_task(failing_task)
    with suppress(ValueError):
        await task

    assert len(subscriber_with_task_mixin.tasks) >= 1


@pytest.mark.asyncio
async def test_task_successful(subscriber_with_task_mixin):
    async def successful_task():
        return True

    task = subscriber_with_task_mixin.add_task(successful_task)
    await task
    assert len(subscriber_with_task_mixin.tasks) == 1
    assert task.result()
