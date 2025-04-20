import pytest

from faststream.nats import TestApp, TestNatsBroker


@pytest.mark.asyncio()
async def test_basic() -> None:
    from docs.docs_src.nats.js.pull_sub import app, broker, handle

    async with TestNatsBroker(broker), TestApp(app):
        await broker.publish("Hi!", "test")
        handle.mock.assert_called_once_with("Hi!")
