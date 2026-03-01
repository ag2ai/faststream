import pytest

from faststream.redis import TestRedisBroker

from .object import broker, publisher


@pytest.mark.asyncio
async def test_handle():
    async with TestRedisBroker(broker) as br:
        await br.publish("", channel="test-channel")

        publisher.mock.assert_called_once_with("Hi!")
