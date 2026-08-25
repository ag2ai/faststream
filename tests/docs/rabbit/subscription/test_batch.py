import asyncio

import pytest

from faststream.rabbit import TestApp, TestRabbitBroker


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_flush_by_batch() -> None:
    from docs.docs_src.rabbit.subscription.batch import (
        BATCH_SIZE,
        app,
        broker,
        collector,
        example_exchange,
        example_queue,
    )

    collector.processed.clear()
    collector.data.clear()
    collector.msg.clear()
    collector._cancel_timer()

    async with TestRabbitBroker(broker), TestApp(app):
        for i in range(BATCH_SIZE):
            await broker.publish(
                message={"msg": i},
                exchange=example_exchange,
                routing_key=example_queue.name,
            )

        assert len(collector.processed) == 1
        assert len(collector.processed[0]) == BATCH_SIZE
        assert collector.data == []


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_flush_by_timeout() -> None:
    from docs.docs_src.rabbit.subscription.batch import (
        app,
        broker,
        collector,
        example_exchange,
        example_queue,
    )

    collector.processed.clear()
    collector.data.clear()
    collector.msg.clear()
    collector._cancel_timer()
    collector.flush_interval = 0.1

    async with TestRabbitBroker(broker), TestApp(app):
        await broker.publish(
            message={"msg": 1},
            exchange=example_exchange,
            routing_key=example_queue.name,
        )

        for _ in range(50):
            if collector.processed:
                break
            await asyncio.sleep(0.05)

        assert len(collector.processed) == 1
        assert collector.processed[0] == [{"msg": 1}]


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_concurrency_batch() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from docs.docs_src.rabbit.subscription.batch import BATCH_SIZE, BatchCollector

    collector = BatchCollector(batch_size=BATCH_SIZE, flush_interval=60.0)
    logger = MagicMock()
    total = BATCH_SIZE * 3

    async def deliver(i: int) -> None:
        raw = AsyncMock()
        await collector.add({"msg": i}, raw, logger)

    await asyncio.gather(*(deliver(i) for i in range(total)))

    assert collector.data == []
    assert len(collector.processed) == 3
    assert all(len(batch) == BATCH_SIZE for batch in collector.processed)
    assert sum(len(batch) for batch in collector.processed) == total
