import pytest
from pydantic import ValidationError

from faststream.exceptions import SetupError
from faststream.rabbit import TestRabbitBroker

from .annotation import broker, handle


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestRabbitBroker(broker) as br:
        await br.publish({"name": "John", "user_id": 1}, queue="test-queue")

        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})

    with pytest.raises(SetupError):  # the mock leaves with the test broker
        handle.mock.assert_not_called()

@pytest.mark.asyncio
async def test_validation_error() -> None:
    async with TestRabbitBroker(broker) as br:
        with pytest.raises(ValidationError):
            await br.publish("wrong message", queue="test-queue")

        handle.mock.assert_called_once_with("wrong message")
