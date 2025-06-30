import pytest
from fast_depends.exceptions import ValidationError

from faststream.nats import TestNatsBroker

from .pydantic_fields import broker, handle


@pytest.mark.asyncio
async def test_handle():
    async with TestNatsBroker(broker, with_real=True) as br:
        await br.publish({"name": "John", "user_id": 1}, subject="test-subject")
        await handle.wait_call(timeout=3)
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})

    assert handle.mock is None

@pytest.mark.asyncio
async def test_validation_error():
    async with TestNatsBroker(broker, with_real=True) as br:
        with pytest.raises(ValidationError):
            await br.publish("wrong message", subject="test-subject")
            await handle.wait_call(timeout=3)

        handle.mock.assert_called_once_with("wrong message")
