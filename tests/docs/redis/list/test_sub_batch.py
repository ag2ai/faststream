import pytest

from faststream.redis import TestApp, TestRedisBroker


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_batch() -> None:
    from docs.docs_src.redis.list.sub_batch import app, broker, handle

    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with(["Hi", "again"])
