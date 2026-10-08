import pytest
from pydantic import ValidationError

from faststream.exceptions import SetupError
from faststream.redis import TestRedisBroker

from .pydantic_fields import broker, handle


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestRedisBroker(broker, with_real=True) as br:
        await br.publish({"name": "John", "user_id": 1}, channel="test-channel")
        await handle.wait_call(timeout=3)
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})

    with pytest.raises(SetupError):  # the mock leaves with the test broker
        handle.mock.assert_not_called()

@pytest.mark.asyncio
async def test_validation_error() -> None:
    async with TestRedisBroker(broker, with_real=True) as br:
        with pytest.raises(ValidationError):
            await br.publish("wrong message", channel="test-channel")
            await handle.wait_call(timeout=3)

        handle.mock.assert_called_once_with("wrong message")
