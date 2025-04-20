import pytest

from faststream.rabbit import TestApp, TestRabbitBroker


@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_index() -> None:
    from docs.docs_src.rabbit.subscription.fanout import (
        app,
        base_handler1,
        base_handler3,
        broker,
    )

    async with TestRabbitBroker(broker, with_real=True), TestApp(app):
        await base_handler1.wait_call(3)
        await base_handler3.wait_call(3)

        base_handler1.mock.assert_called_with(b"")
        base_handler3.mock.assert_called_with(b"")
