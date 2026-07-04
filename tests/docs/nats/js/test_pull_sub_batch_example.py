import pytest

from faststream.nats import TestApp, TestNatsBroker


@pytest.mark.nats()
@pytest.mark.asyncio()
async def test_batch() -> None:
    from docs.docs_src.nats.js.pull_sub_batch_example import app, broker, handler

    async with TestNatsBroker(broker), TestApp(app):
        assert handler.mock.call_count == 3
        handler.mock.assert_called_with(["Hello, world! 2"])
