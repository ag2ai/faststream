import pytest
from pydantic import ValidationError
from faststream import FastStream
from faststream.kafka import KafkaBroker, TestKafkaBroker


broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@pytest.mark.asyncio
async def test_correct() -> None:
    async with TestKafkaBroker(broker) as br:
        await br.publish(...)

@pytest.mark.asyncio
async def test_invalid() -> None:
    async with TestKafkaBroker(broker) as br:
        with pytest.raises(ValidationError): ...
