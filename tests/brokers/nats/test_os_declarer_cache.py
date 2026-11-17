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
class TestObjectStorageCache:
    async def test_object_storage_cache(self) -> None:
        broker = NatsBroker()
        async with broker:
            with patch.object(
                JetStreamContext,
                "create_object_store",
                spy_decorator(JetStreamContext.create_object_store),
            ) as m:
                await broker.object_storage("test")
                await broker.object_storage("test")
                assert broker.config.os_declarer.buckets["test"]
                m.mock.assert_called_once()

    async def test_object_storage_reconnect(self, settings: Settings, queue: str) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3331."""
        broker = NatsBroker(settings.url)
        async with broker:
            bucket = await broker.object_storage(queue)
            await bucket.put("key", b"original")

        async with broker:
            bucket = await broker.object_storage(queue)
            assert (await bucket.get("key")).data == b"original"
            await bucket.put("key", b"updated")
            assert (await bucket.get("key")).data == b"updated"

    async def test_object_storage_reconnect_without_declaration(
        self,
        settings: Settings,
        queue: str,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3331."""
        broker = NatsBroker(settings.url)
        async with broker:
            await broker.object_storage(queue)

        async with broker:
            bucket = await broker.object_storage(queue, declare=False)
            await bucket.put("key", b"reconnected")
            assert (await bucket.get("key")).data == b"reconnected"

    async def test_object_storage_access_after_disconnect(
        self,
        settings: Settings,
        queue: str,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3331."""
        broker = NatsBroker(settings.url)
        async with broker:
            await broker.object_storage(queue)

        with pytest.raises(IncorrectState):
            await broker.object_storage(queue)
