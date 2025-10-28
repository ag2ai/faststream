import pytest

from faststream.redis import TestApp, TestRedisBroker


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_stream_claiming_basic() -> None:
    from docs.docs_src.redis.stream.claiming_basic import app, broker, handle

    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("order-123")


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_stream_claiming_manual_ack() -> None:
    from docs.docs_src.redis.stream.claiming_manual_ack import app, broker, handle

    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("critical-task-1")
