import pytest

from faststream.rabbit import TestApp, TestRabbitBroker


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_dead_letter() -> None:
    from docs.docs_src.rabbit.dead_letter import (
        app,
        broker,
        handle_dead_letter,
        handle_order,
    )

    async with TestRabbitBroker(broker, with_real=True), TestApp(app):
        await broker.publish("bad-order", "orders")

        await handle_dead_letter.wait_call(3)

        handle_order.mock.assert_called_once_with("bad-order")
        handle_dead_letter.mock.assert_called_once_with("bad-order")
