from unittest.mock import AsyncMock

import pytest
from aio_pika import RobustQueue

from faststream import TestApp
from tests.marks import require_aiopika


@pytest.mark.asyncio()
@pytest.mark.rabbit()
@require_aiopika
async def test_bind(monkeypatch, async_mock: AsyncMock):
    from docs.docs_src.rabbit.bind import app, broker, some_exchange, some_queue

    with monkeypatch.context() as m:
        m.setattr(RobustQueue, "bind", async_mock)

        async with TestApp(app):
            assert len(broker.declarer._RabbitDeclarer__queues) == 2  # with `reply-to`
            assert len(broker.declarer._RabbitDeclarer__exchanges) == 1

            assert some_queue in broker.declarer._RabbitDeclarer__queues
            assert some_exchange in broker.declarer._RabbitDeclarer__exchanges

            row_exchange = await broker.declarer.declare_exchange(some_exchange)
            async_mock.assert_awaited_once_with(
                exchange=row_exchange,
                routing_key=some_queue.name,
            )
