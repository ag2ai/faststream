import pytest

from faststream import TestApp
from faststream.nats import TestNatsBroker


@pytest.mark.asyncio()
async def test_basic() -> None:
    from examples.nats.e08_wildcards import app, broker, handler, handler_match

    async with TestNatsBroker(broker), TestApp(app):
        handler.mock.assert_called_once_with("Hi!")
        handler_match.mock.assert_called_with("Hi!")
        assert handler_match.mock.call_count == 2
