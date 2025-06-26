import pytest

from docs.docs_src.confluent.publish_with_partition_key.app import (
    Data,
    broker,
    on_input_data,
    to_output_data,
)
from faststream.confluent import TestKafkaBroker


@pytest.mark.asyncio()
async def test_app() -> None:
    async with TestKafkaBroker(broker):
        await broker.publish(Data(data=0.2), "input_data", key=b"my_key")

        on_input_data.mock.assert_called_once_with(dict(Data(data=0.2)))
        to_output_data.mock.assert_called_once_with(dict(Data(data=1.2)))


@pytest.mark.skip("we are not checking the key")
@pytest.mark.asyncio()
async def test_keys() -> None:
    async with TestKafkaBroker(broker):
        # we should be able to publish a message with the key
        await broker.publish(Data(data=0.2), "input_data", key=b"my_key")

        # we need to check the key as well
        on_input_data.mock.assert_called_once_with(dict(Data(data=0.2)), key=b"my_key")
        to_output_data.mock.assert_called_once_with(dict(Data(data=1.2)), key=b"key")
