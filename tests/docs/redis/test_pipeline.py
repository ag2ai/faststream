import pytest

from faststream.redis import TestApp, TestRedisBroker


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_pipeline() -> None:
    from docs.docs_src.redis.pipeline.pipeline import (
        app,
        broker,
        handle,
    )

    broker.config.fd_config.serializer = None
    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("Hi!")
