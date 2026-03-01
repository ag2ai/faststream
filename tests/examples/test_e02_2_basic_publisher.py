import pytest

from tests.marks import require_aiopika


@pytest.mark.asyncio()
@require_aiopika
async def test_example() -> None:
    from examples.e02_2_basic_publisher import app, broker, handle, handle_response
    from faststream.rabbit import TestApp, TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app):
        await handle.wait_call(3)
        await handle_response.wait_call(3)

        handle.mock.assert_called_with("Hello!")
        handle_response.mock.assert_called_with("Response")
