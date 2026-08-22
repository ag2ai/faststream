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
