import pytest

from docs.docs_src.kafka.topic_configuration.app import broker, on_order
from faststream.kafka import TestKafkaBroker


@pytest.mark.kafka()
@pytest.mark.asyncio()
@pytest.mark.parametrize("topic", ("orders", "legacy-orders", "audit"))
async def test_app(topic: str) -> None:
    async with TestKafkaBroker(broker):
        await broker.publish("hello", topic)
        on_order.mock.assert_called_with("hello")
