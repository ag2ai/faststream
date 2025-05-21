import pytest

from docs.docs_src.confluent.publish_example.app import (
    Data,
    broker,
    on_input_data,
    to_output_data,
)
from faststream.confluent import TestKafkaBroker


@pytest.mark.asyncio()
async def test_base_app() -> None:
    async with TestKafkaBroker(broker):
        await broker.publish(Data(data=0.2), "input_data")

        on_input_data.mock.assert_called_once_with(dict(Data(data=0.2)))
        to_output_data.mock.assert_called_once_with(dict(Data(data=1.2)))
