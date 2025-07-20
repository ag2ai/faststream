import pytest

from faststream.redis import TestRedisBroker


@pytest.mark.asyncio()
async def test_pattern_data() -> None:
    from docs.docs_src.redis.pub_sub.pattern_data import broker, handle_test

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", "test.channel")
        handle_test.mock.assert_called_once_with("Hi!")
