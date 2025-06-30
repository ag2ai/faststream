import pytest

from faststream.redis import TestRedisBroker


@pytest.mark.asyncio()
async def test_publisher() -> None:
    from docs.docs_src.redis.pub_sub.publisher_decorator import (
        broker,
        on_input_data,
        to_output_data,
    )

    async with TestRedisBroker(broker) as br:
        await br.publish({"data": 1.0}, "input_data")
        on_input_data.mock.assert_called_once_with({"data": 1.0})
        to_output_data.mock.assert_called_once_with({"data": 2.0})
