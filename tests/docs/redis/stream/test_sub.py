import pytest

from faststream.redis import TestRedisBroker


@pytest.mark.asyncio()
async def test_stream_sub() -> None:
    from docs.docs_src.redis.stream.sub import broker, handle

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", stream="test-stream")
        handle.mock.assert_called_once_with("Hi!")
