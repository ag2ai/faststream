import pytest

from faststream.redis import TestRedisBroker


@pytest.mark.asyncio()
async def test_list() -> None:
    from docs.docs_src.redis.list.list_sub import broker, handle

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", list="test-list")
        handle.mock.assert_called_once_with("Hi!")
