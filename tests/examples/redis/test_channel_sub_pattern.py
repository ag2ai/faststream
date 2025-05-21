import pytest

from faststream.redis import TestApp, TestRedisBroker


@pytest.mark.asyncio()
async def test_pattern() -> None:
    from examples.redis.channel_sub_pattern import app, broker, handle_test

    async with TestRedisBroker(broker), TestApp(app):
        handle_test.mock.assert_called_once_with("Hi!")
