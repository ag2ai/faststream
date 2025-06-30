import pytest

from faststream.redis import TestApp, TestRedisBroker


@pytest.mark.asyncio()
async def test_rpc() -> None:
    from examples.redis.rpc import (
        app,
        broker,
    )

    async with TestRedisBroker(broker), TestApp(app):
        pass
