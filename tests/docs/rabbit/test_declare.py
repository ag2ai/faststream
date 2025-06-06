import pytest

from faststream import TestApp


@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_declare() -> None:
    from docs.docs_src.rabbit.declare import app, broker

    async with TestApp(app):
        assert len(broker.config.declarer._exchanges) == 1
        assert len(broker.config.declarer._queues) == 2  # with `reply-to`
