import pytest
from pydantic import ValidationError
from faststream import FastStream
from faststream.redis import RedisBroker, TestRedisBroker


broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


@pytest.mark.asyncio
async def test_correct() -> None:
    async with TestRedisBroker(broker) as br:
        await br.publish(...)

@pytest.mark.asyncio
async def test_invalid() -> None:
    async with TestRedisBroker(broker) as br:
        with pytest.raises(ValidationError): ...