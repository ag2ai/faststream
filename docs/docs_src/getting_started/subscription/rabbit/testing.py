import pytest
from fast_depends.exceptions import ValidationError

from faststream.rabbit import TestRabbitBroker

from .annotation import broker, handle


@pytest.mark.asyncio
async def test_handle():
    async with TestRabbitBroker(broker) as br:
        await br.publish({"name": "John", "user_id": 1}, queue="test-queue")

        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})

    assert handle.mock is None

@pytest.mark.asyncio
async def test_validation_error():
    async with TestRabbitBroker(broker) as br:
        with pytest.raises(ValidationError):
            await br.publish("wrong message", queue="test-queue")

        handle.mock.assert_called_once_with("wrong message")
