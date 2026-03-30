import pytest

from faststream import TestApp
from faststream.mq import TestMQBroker


@pytest.mark.mq()
@pytest.mark.asyncio()
async def test_rpc() -> None:
    from docs.docs_src.mq.rpc import app, broker

    async with TestMQBroker(broker), TestApp(app):
        pass
