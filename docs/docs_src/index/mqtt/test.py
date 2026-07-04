from .pydantic import broker

import pytest
from pydantic import ValidationError
from faststream.mqtt import TestMQTTBroker


@pytest.mark.asyncio
async def test_correct() -> None:
    async with TestMQTTBroker(broker) as br:
        await br.publish(
            {
                "user": "John",
                "user_id": 1,
            }, "in-topic",
        )

@pytest.mark.asyncio
async def test_invalid() -> None:
    async with TestMQTTBroker(broker) as br:
        with pytest.raises(ValidationError):
            await br.publish("wrong message", "in-topic")
