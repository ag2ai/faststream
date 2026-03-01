import pytest

from examples.router.basic_publish import app, broker, handle, handle_response
from faststream.kafka import TestApp, TestKafkaBroker


@pytest.mark.asyncio()
async def test_example() -> None:
    async with TestKafkaBroker(broker), TestApp(app):
        await handle.wait_call(3)
        await handle_response.wait_call(3)

        handle.mock.assert_called_with("Hello!")
        handle_response.mock.assert_called_with("Response")
