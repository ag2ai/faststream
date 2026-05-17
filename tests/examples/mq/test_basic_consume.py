import pytest

from faststream import TestApp
from faststream.mq import TestMQBroker


@pytest.mark.mq()
@pytest.mark.asyncio()
async def test_basic_consume() -> None:
    from examples.mq.basic_consume import app, broker, handle

    async with TestMQBroker(broker), TestApp(app):
        await handle.wait_call(3)
        handle.mock.assert_called_with("Hello!")
