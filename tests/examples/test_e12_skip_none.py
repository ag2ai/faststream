from unittest.mock import call

import pytest

from examples.e12_skip_none import app, broker, handle, handle_response, publisher
from faststream.rabbit import TestApp, TestRabbitBroker
from tests.marks import require_aiopika


@pytest.mark.asyncio()
@require_aiopika
async def test_example() -> None:

    async with TestRabbitBroker(broker), TestApp(app):
        await handle.wait_call(3)
        await handle_response.wait_call(3)

        handle.mock.assert_has_calls([call("Hello!"), call("ignore")])

        # the `None` return value was skipped
        handle_response.mock.assert_called_once_with("Processed: Hello!")
        publisher.mock.assert_called_once_with("Processed: Hello!")
