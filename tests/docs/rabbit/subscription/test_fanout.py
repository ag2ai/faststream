import pytest

from faststream.rabbit import TestApp, TestRabbitBroker


@pytest.mark.asyncio
@pytest.mark.rabbit
async def test_index():
    from docs.docs_src.rabbit.subscription.fanout import (
        app,
        broker,
        base_handler1,
        base_handler3,
    )

    async with TestRabbitBroker(broker, with_real=True, connect_only=True):
        async with TestApp(app):
            await base_handler1.wait_call(3)
            await base_handler3.wait_call(3)

            base_handler1.mock.assert_called_with("")
            base_handler3.mock.assert_called_with("")
