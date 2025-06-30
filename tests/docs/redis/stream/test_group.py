import pytest

from faststream.redis import TestApp, TestRedisBroker


@pytest.mark.asyncio()
async def test_stream_group() -> None:
    from docs.docs_src.redis.stream.group import app, broker, handle

    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("Hi!")
