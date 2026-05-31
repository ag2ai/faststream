import pytest

from faststream.mqtt import TestMQTTBroker

from .object import broker, publisher


@pytest.mark.asyncio
async def test_handle():
    async with TestMQTTBroker(broker) as br:
        await br.publish("", topic="test-topic")

        publisher.mock.assert_called_once_with("Hi!")
