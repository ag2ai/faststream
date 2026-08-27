import asyncio

import pytest

from faststream.rabbit import TestApp, TestRabbitBroker


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_flush_by_batch() -> None:
    from docs.docs_src.rabbit.subscription.batch import (
        BATCH_SIZE,
        app,
        batch,
        broker,
        processed,
        queue,
    )

    processed.clear()
    batch.clear()
    async with TestRabbitBroker(broker), TestApp(app):
        for i in range(BATCH_SIZE):
            await broker.publish({"msg": i}, queue.name)
        assert len(processed) == 1
        assert len(processed[0]) == BATCH_SIZE
        assert batch == []


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_concurrency_batch() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from docs.docs_src.rabbit.subscription.batch import (
        BATCH_SIZE,
        batch,
        flush,
        lock,
        processed,
        take_batch,
    )

    processed.clear()
    batch.clear()
    logger = MagicMock()
    total = BATCH_SIZE * 3

    async def deliver(i: int) -> None:
        items = None
        async with lock:
            batch.append(({"msg": i}, AsyncMock()))
            if len(batch) >= BATCH_SIZE:
                items = take_batch()
        if items is not None:
            await flush(items, logger)

    await asyncio.gather(*(deliver(i) for i in range(total)))
    assert batch == []
    assert len(processed) == 3
    assert all(len(b) == BATCH_SIZE for b in processed)
    assert sum(len(b) for b in processed) == total
