import pytest
from pydantic import ValidationError
from faststream import FastStream
from faststream.nats import NatsBroker, TestNatsBroker


broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)


@pytest.mark.asyncio
async def test_correct() -> None:
    async with TestNatsBroker(broker) as br:
        await br.publish(...)

@pytest.mark.asyncio
async def test_invalid() -> None:
    async with TestNatsBroker(broker) as br:
        with pytest.raises(ValidationError): ...
