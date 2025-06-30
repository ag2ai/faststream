import pytest

from faststream.redis import TestApp, TestRedisBroker


@pytest.mark.asyncio()
async def test_list() -> None:
    from examples.redis.list_sub import app, broker, handle

    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("Hi!")
