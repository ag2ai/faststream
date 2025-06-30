import pytest

from examples.kafka.batch_consume import app, broker, handle
from faststream.kafka import TestApp, TestKafkaBroker


@pytest.mark.asyncio()
async def test_example() -> None:
    async with TestKafkaBroker(broker), TestApp(app):
        await handle.wait_call(3)
        assert set(handle.mock.call_args[0][0]) == {"hi", "FastStream"}
