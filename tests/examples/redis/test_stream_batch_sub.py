import pytest

from faststream.redis import TestApp, TestRedisBroker


@pytest.mark.asyncio()
async def test_stream_batch() -> None:
    from examples.redis.stream_sub_batch import app, broker, handle

    async with TestRedisBroker(broker), TestApp(app):
        assert handle.mock.call_count == 3
