import pytest

from faststream.redis import TestApp, TestRedisBroker
from tests.marks import python39


@pytest.mark.asyncio()
@python39
async def test_stream_batch() -> None:
    from examples.redis.stream_sub_batch import app, broker, handle

    async with TestRedisBroker(broker), TestApp(app):
        assert handle.mock.call_count == 3
