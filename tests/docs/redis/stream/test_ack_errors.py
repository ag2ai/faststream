from unittest.mock import patch

import pytest
from redis.asyncio import Redis

from faststream.redis import TestApp, TestRedisBroker
from tests.tools import spy_decorator


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_stream_ack() -> None:
    from docs.docs_src.redis.stream.ack_errors import app, broker, handle

    with patch.object(Redis, "xack", spy_decorator(Redis.xack)) as m:
        async with TestRedisBroker(broker, with_real=True), TestApp(app):
            await handle.wait_call(3)
            handle.mock.assert_called_once_with("Hello World!")

        m.mock.assert_called()
