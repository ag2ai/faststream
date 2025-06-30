import pytest

from faststream import TestApp
from faststream.nats import TestNatsBroker


@pytest.mark.asyncio()
async def test_basic() -> None:
    from examples.nats.e05_basic_and_js import app, broker, core_handler, js_handler

    async with TestNatsBroker(broker), TestApp(app):
        core_handler.mock.assert_called_once_with("Hi!")
        js_handler.mock.assert_called_once_with("Hi!")
