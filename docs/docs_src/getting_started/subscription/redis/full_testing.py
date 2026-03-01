import pytest
from faststream.redis import RedisBroker, TestRedisBroker

broker = RedisBroker("redis://localhost:6379")


@broker.subscriber("test-channel")
async def handle(msg: str) -> None:
    raise ValueError


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestRedisBroker(broker) as br:
        with pytest.raises(ValueError):
            await br.publish("hello!", "test-channel")
