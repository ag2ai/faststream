import pytest

from faststream.redis import TestRedisBroker


@pytest.mark.asyncio()
async def test_channel() -> None:
    from docs.docs_src.redis.pub_sub.channel_sub import broker, handle

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", "test")
        handle.mock.assert_called_once_with("Hi!")
