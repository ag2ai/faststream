import pytest

from faststream.redis import TestRedisBroker


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_list_publisher() -> None:
    from docs.docs_src.redis.list.list_pub import broker, on_input_data

    publisher = list(broker.publishers)[0]  # ruff: ignore[unnecessary-iterable-allocation-for-first-element]

    async with TestRedisBroker(broker) as br:
        await br.publish({"data": 1.0}, list="input-list")
        on_input_data.mock.assert_called_once_with({"data": 1.0})
        publisher.mock.assert_called_once_with({"data": 2.0})
