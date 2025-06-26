import pytest

from tests.marks import require_aiopika


@pytest.mark.asyncio()
@require_aiopika
async def test_example() -> None:
    from examples.e03_miltiple_pubsub import (
        app,
        broker,
        handle,
        handle_response_1,
        handle_response_2,
    )
    from faststream.rabbit import TestApp, TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app):
        await handle.wait_call(3)
        await handle_response_1.wait_call(3)
        await handle_response_2.wait_call(3)

        handle.mock.assert_called_with("Hello!")
        handle_response_1.mock.assert_called_with("Response")
        handle_response_2.mock.assert_called_with("Response")
