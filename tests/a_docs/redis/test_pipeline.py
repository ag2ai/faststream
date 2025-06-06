import pytest

from faststream.redis import TestRedisBroker
from faststream.testing.app import TestApp


@pytest.mark.asyncio
@pytest.mark.pipe
async def test_pipeline():
    from docs.docs_src.redis.pipeline.pipeline import (
        app,
        broker,
        handle,
    )

    broker._is_validate = False
    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("Hi!")
