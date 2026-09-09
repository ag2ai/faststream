import pytest

from faststream.rabbit import TestRabbitBroker

from .object import broker, publisher


@pytest.mark.asyncio
async def test_handle():
    async with TestRabbitBroker(broker) as br:
        await br.publish("", queue="test-queue")

        publisher.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio
async def test_message_fields():
    async with TestRabbitBroker(broker) as br:
        await br.publish("", queue="test-queue", correlation_id="42")

        await publisher.assert_called_once_with("Hi!", correlation_id="42")
