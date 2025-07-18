import pytest

from faststream import TestApp
from faststream.nats import TestNatsBroker


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.nats()
@pytest.mark.flaky(reruns=3, reruns_delay=1)
async def test_basic() -> None:
    from docs.docs_src.nats.js.key_value import app, broker, handler

    async with TestNatsBroker(broker, with_real=True), TestApp(app):
        await handler.wait_call(3.0)
        handler.mock.assert_called_once_with(b"Hello!")
