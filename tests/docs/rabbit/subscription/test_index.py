import pytest

from faststream.rabbit import TestApp, TestRabbitBroker


@pytest.mark.asyncio()
async def test_index() -> None:
    from docs.docs_src.rabbit.subscription.index import app, broker, handle

    async with TestRabbitBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("message")
