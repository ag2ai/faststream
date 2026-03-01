import pytest

from tests.marks import require_aiopika


@pytest.mark.asyncio()
@require_aiopika
async def test_example() -> None:
    from examples.e10_middlewares import app, broker, handle
    from faststream.rabbit import TestApp, TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app):
        await handle.wait_call(3)

        handle.mock.assert_called_with("fake message")
