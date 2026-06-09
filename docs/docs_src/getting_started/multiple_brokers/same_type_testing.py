import pytest

from faststream.kafka import TestKafkaBroker

from .same_type_app import broker_1, broker_2, from_first, from_second


@pytest.mark.asyncio()
async def test_bridge() -> None:
    async with TestKafkaBroker(broker_1, broker_2) as (br1, _):
        await br1.publish("Hi!", "incoming")

        from_first.mock.assert_called_once_with("Hi!")
        from_second.mock.assert_called_once_with("Hi!")
