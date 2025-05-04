import pytest

from faststream import TestApp
from faststream.nats import TestNatsBroker


@pytest.mark.asyncio()
async def test_basic() -> None:
    from examples.nats.e04_js_basic import app, broker, handler

    async with TestNatsBroker(broker), TestApp(app):
        assert handler.mock.call_count == 2
        handler.mock.assert_called_with("Hi!")
