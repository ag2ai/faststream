import pytest

from faststream import TestApp
from faststream.nats import TestNatsBroker


@pytest.mark.asyncio()
async def test_main() -> None:
    from docs.docs_src.nats.js.main import app, broker, handler

    async with TestNatsBroker(broker), TestApp(app):
        assert handler.mock.call_count == 2
