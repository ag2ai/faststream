import pytest

from docs.docs_src.mqtt.basic import broker, on_temp, publish_demo
from faststream.mqtt import TestMQTTBroker


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_basic_example() -> None:
    async with TestMQTTBroker(broker):
        await publish_demo()
        on_temp.mock.assert_called_once_with(21.5)
