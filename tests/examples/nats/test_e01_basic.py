import pytest

from faststream import TestApp
from faststream.nats import TestNatsBroker


@pytest.mark.asyncio()
async def test_basic() -> None:
    from examples.nats.e01_basic import app, broker, handler

    async with TestNatsBroker(broker), TestApp(app):
        handler.mock.assert_called_once_with("Hi!")
