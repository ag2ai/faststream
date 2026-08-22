from unittest.mock import call

import pytest

from examples.mqtt.skip_none_retained import app, broker, handle_status
from faststream import TestApp
from faststream.mqtt import TestMQTTBroker


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_example() -> None:
    async with TestMQTTBroker(broker), TestApp(app):
        await handle_status.wait_call(3)

        # "online", the clearing empty payload, "online" again:
        # the `skip_none` publisher did not deliver anything
        handle_status.mock.assert_has_calls(
            [call("online"), call(b""), call("online")]
        )
