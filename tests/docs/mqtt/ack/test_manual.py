import pytest

from docs.docs_src.mqtt.ack.manual import broker, work
from faststream.mqtt import TestMQTTBroker


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_manual_ack_example() -> None:
    async with TestMQTTBroker(broker):
        await broker.publish({"job": "build"}, "jobs/run")
        work.mock.assert_called_once_with({"job": "build"})
