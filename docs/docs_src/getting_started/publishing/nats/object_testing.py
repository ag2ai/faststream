import pytest

from faststream.nats import TestNatsBroker

from .object import broker, publisher


@pytest.mark.asyncio
async def test_handle():
    async with TestNatsBroker(broker) as br:
        await br.publish("", subject="test-subject")

        publisher.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio
async def test_message_fields():
    async with TestNatsBroker(broker) as br:
        await br.publish("", subject="test-subject", correlation_id="42")

        await publisher.assert_called_once_with("Hi!", correlation_id="42")
