import pytest

from faststream.redis import TestApp, TestRedisBroker


@pytest.mark.asyncio()
async def test_stream_sub() -> None:
    from examples.redis.stream_sub import app, broker, handle

    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("Hi!")
