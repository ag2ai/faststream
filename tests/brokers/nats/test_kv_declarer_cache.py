from unittest.mock import patch

import pytest
from nats.js import JetStreamContext

from faststream.exceptions import IncorrectState
from faststream.nats import NatsBroker
from tests.brokers.nats.settings import Settings
from tests.tools import spy_decorator


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_kv_storage_cache() -> None:
    broker = NatsBroker()
    async with broker:
        with patch.object(
            JetStreamContext,
            "create_key_value",
            spy_decorator(JetStreamContext.create_key_value),
        ) as m:
            await broker.key_value("test")
            await broker.key_value("test")
            assert broker.config.kv_declarer.buckets["test"]
            m.mock.assert_called_once()


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_kv_storage_reconnect(settings: Settings, queue: str) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/3331."""
    broker = NatsBroker(settings.url)
    async with broker:
        bucket = await broker.key_value(queue)
        await bucket.put("key", b"original")

    async with broker:
        bucket = await broker.key_value(queue)
        assert (await bucket.get("key")).value == b"original"
        await bucket.put("key", b"updated")
        assert (await bucket.get("key")).value == b"updated"


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_kv_storage_reconnect_without_declaration(
    settings: Settings,
    queue: str,
) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/3331."""
    broker = NatsBroker(settings.url)
    async with broker:
        await broker.key_value(queue)

    async with broker:
        bucket = await broker.key_value(queue, declare=False)
        await bucket.put("key", b"reconnected")
        assert (await bucket.get("key")).value == b"reconnected"


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_kv_storage_access_after_disconnect(settings: Settings, queue: str) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/3331."""
    broker = NatsBroker(settings.url)
    async with broker:
        await broker.key_value(queue)

    with pytest.raises(IncorrectState):
        await broker.key_value(queue)
