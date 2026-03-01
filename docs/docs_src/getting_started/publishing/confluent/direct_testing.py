import pytest

from faststream.confluent import TestKafkaBroker

from .direct import broker, publisher


@pytest.mark.asyncio
async def test_handle():
    async with TestKafkaBroker(broker) as br:
        await br.publish("", topic="test-topic")

        publisher.mock.assert_called_once_with("Hi!")
