import pytest
from pydantic import ValidationError

from faststream.kafka import TestKafkaBroker

from .annotation import broker, handle


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestKafkaBroker(broker) as br:
        await br.publish({"name": "John", "user_id": 1}, topic="test-topic")

        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})

    assert not handle.mock.called  # mock is reset

@pytest.mark.asyncio
async def test_validation_error() -> None:
    async with TestKafkaBroker(broker) as br:
        with pytest.raises(ValidationError):
            await br.publish("wrong message", topic="test-topic")

        handle.mock.assert_called_once_with("wrong message")
