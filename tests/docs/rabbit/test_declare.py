import pytest

from faststream import TestApp


@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_declare() -> None:
    from docs.docs_src.rabbit.declare import app, broker

    async with TestApp(app):
        assert len(broker.declarer._RabbitDeclarer__exchanges) == 1
        assert len(broker.declarer._RabbitDeclarer__queues) == 2  # with `reply-to`
