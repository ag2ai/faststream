import pytest

from faststream.nats import TestNatsBroker

from .direct import broker, publisher


@pytest.mark.asyncio
async def test_handle():
    async with TestNatsBroker(broker) as br:
        await br.publish("", subject="test-subject")

        publisher.mock.assert_called_once_with("Hi!")
