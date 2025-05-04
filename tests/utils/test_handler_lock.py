import asyncio

import anyio
import pytest
from anyio.abc import TaskStatus

from faststream._internal.subscriber.utils import MultiLock


@pytest.mark.asyncio()
async def test_base() -> None:
    lock = MultiLock()

    with lock:
        assert not lock.empty
        assert lock.qsize == 1

        with lock:
            assert not lock.empty
            assert lock.qsize == 2

        assert not lock.empty
        assert lock.qsize == 1

    assert lock.empty
    assert lock.qsize == 0


@pytest.mark.asyncio()
async def test_wait_correct() -> None:
    lock = MultiLock()

    async def func() -> None:
        with lock:
            await asyncio.sleep(0.01)

    async def check(task_status: TaskStatus) -> None:
        task_status.started()

        assert not lock.empty
        assert lock.qsize == 1

        await lock.wait_release(5)

        assert lock.empty
        assert lock.qsize == 0

    async with anyio.create_task_group() as tg:
        tg.start_soon(func)
        await tg.start(check)


@pytest.mark.asyncio()
async def test_nowait_correct() -> None:
    lock = MultiLock()

    async def func() -> None:
        with lock:
            await asyncio.sleep(0.01)

    async def check(task_status: TaskStatus) -> None:
        task_status.started()

        assert not lock.empty
        assert lock.qsize == 1

        await lock.wait_release()

        assert not lock.empty
        assert lock.qsize == 1

    async with anyio.create_task_group() as tg:
        tg.start_soon(func)
        await tg.start(check)
