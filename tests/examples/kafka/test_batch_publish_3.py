import pytest

from examples.kafka.batch_publish_3 import app, handle, handle_response, broker
from faststream.kafka import TestApp, TestKafkaBroker


@pytest.mark.asyncio
async def test_example():
    async with TestKafkaBroker(broker, connect_only=True):
        async with TestApp(app):
            await handle.wait_call(3)
            await handle_response.wait_call(3)

        handle.mock.assert_called_with("hi")
        assert set(handle_response.mock.call_args[0][0]) == {"hi", "FastStream"}
