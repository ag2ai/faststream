from .pydantic import broker

import pytest
from fast_depends.exceptions import ValidationError
from faststream.rabbit import TestRabbitBroker


@pytest.mark.asyncio
async def test_correct():
    async with TestRabbitBroker(broker) as br:
        await br.publish({
            "user": "John",
            "user_id": 1,
        }, "in-queue")

@pytest.mark.asyncio
async def test_invalid():
    async with TestRabbitBroker(broker) as br:
        with pytest.raises(ValidationError):
            await br.publish("wrong message", "in-queue")
