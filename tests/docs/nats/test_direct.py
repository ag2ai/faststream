import pytest

from faststream.nats import TestApp, TestNatsBroker


@pytest.mark.asyncio()
async def test_direct() -> None:
    from docs.docs_src.nats.direct import (
        app,
        base_handler1,
        base_handler2,
        base_handler3,
        broker,
    )

    async with TestNatsBroker(broker), TestApp(app):
        assert base_handler1.mock.call_count == 2
        assert base_handler2.mock.call_count == 0
        assert base_handler3.mock.call_count == 1
