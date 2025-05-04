from .pydantic import broker

import pytest
from fast_depends.exceptions import ValidationError
from faststream.nats import TestNatsBroker


@pytest.mark.asyncio
async def test_correct():
    async with TestNatsBroker(broker) as br:
        await br.publish({
            "user": "John",
            "user_id": 1,
        }, "in-subject")

@pytest.mark.asyncio
async def test_invalid():
    async with TestNatsBroker(broker) as br:
        with pytest.raises(ValidationError):
            await br.publish("wrong message", "in-subject")
