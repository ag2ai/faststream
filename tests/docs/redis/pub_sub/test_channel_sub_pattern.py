import pytest

from faststream.redis import TestRedisBroker


@pytest.mark.asyncio()
async def test_pattern() -> None:
    from docs.docs_src.redis.pub_sub.channel_sub_pattern import broker, handle_test

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", "test.channel")
        handle_test.mock.assert_called_once_with("Hi!")
