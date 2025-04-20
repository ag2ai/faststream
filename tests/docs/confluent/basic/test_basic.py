import pytest

from faststream.confluent import TestKafkaBroker


@pytest.mark.asyncio()
async def test_basic() -> None:
    from docs.docs_src.confluent.basic.basic import broker, on_input_data

    publisher = broker._publishers[0]

    async with TestKafkaBroker(broker) as br:
        await br.publish({"data": 1.0}, "input_data")
        on_input_data.mock.assert_called_once_with({"data": 1.0})
        publisher.mock.assert_called_once_with({"data": 2.0})
