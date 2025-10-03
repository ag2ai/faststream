import pytest
from faststream.nats import NatsBroker, TestNatsBroker

broker = NatsBroker("nats://localhost:4222")


@broker.subscriber("test-subject")
async def handle(msg: str) -> None:
    raise ValueError


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestNatsBroker(broker) as br:
        with pytest.raises(ValueError):
            await br.publish("hello!", "test-subject")
