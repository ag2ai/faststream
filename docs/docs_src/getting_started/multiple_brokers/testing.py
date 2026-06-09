import pytest

from faststream.kafka import TestKafkaBroker
from faststream.nats import TestNatsBroker

from .app import from_kafka, from_nats, kafka_broker, nats_broker


@pytest.mark.asyncio()
async def test_bridge() -> None:
    async with (
        TestKafkaBroker(kafka_broker) as br,
        TestNatsBroker(nats_broker),
    ):
        await br.publish("Hi!", "incoming")

        from_kafka.mock.assert_called_once_with("Hi!")
        from_nats.mock.assert_called_once_with("Hi!")
