import pytest

from faststream.rabbit import TestApp, TestRabbitBroker


@pytest.mark.asyncio()
async def test_index() -> None:
    from examples.rabbit.header import (
        app,
        base_handler1,
        base_handler3,
        broker,
    )

    async with TestRabbitBroker(broker), TestApp(app):
        assert base_handler1.mock.call_count == 3
        assert base_handler3.mock.call_count == 3
