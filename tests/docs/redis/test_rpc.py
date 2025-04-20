import pytest

from faststream.redis import TestApp, TestRedisBroker


@pytest.mark.asyncio()
async def test_rpc() -> None:
    from docs.docs_src.redis.rpc.app import (
        app,
        broker,
    )

    async with TestRedisBroker(broker), TestApp(app):
        pass
