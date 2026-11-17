from unittest.mock import patch

import pytest
from nats.js import JetStreamContext

from faststream.exceptions import IncorrectState
from tests.tools import spy_decorator

from .basic import NatsTestcaseConfig
from .settings import Settings


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.nats()
class TestObjectStorageCache(NatsTestcaseConfig):
    async def test_object_storage_cache(self, settings: Settings, queue: str) -> None:
        broker = self.get_broker(servers=settings.url)
        async with self.patch_broker(broker) as br:
            with patch.object(
                JetStreamContext,
                "create_object_store",
                spy_decorator(JetStreamContext.create_object_store),
            ) as m:
                await br.object_storage(queue)
                await br.object_storage(queue)
                assert br.config.os_declarer.buckets[queue]
                m.mock.assert_called_once()

    async def test_object_storage_reconnect(self, settings: Settings, queue: str) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3331."""
        broker = self.get_broker(servers=settings.url)
        async with self.patch_broker(broker) as br:
            bucket = await br.object_storage(queue)
            await bucket.put("key", b"original")

        async with self.patch_broker(broker) as br:
            bucket = await br.object_storage(queue)
            assert (await bucket.get("key")).data == b"original"
            await bucket.put("key", b"updated")
            assert (await bucket.get("key")).data == b"updated"

    async def test_object_storage_reconnect_without_declaration(
        self,
        settings: Settings,
        queue: str,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3331."""
        broker = self.get_broker(servers=settings.url)
        async with self.patch_broker(broker) as br:
            await br.object_storage(queue)

        async with self.patch_broker(broker) as br:
            bucket = await br.object_storage(queue, declare=False)
            await bucket.put("key", b"reconnected")
            assert (await bucket.get("key")).data == b"reconnected"

    async def test_object_storage_access_after_disconnect(
        self,
        settings: Settings,
        queue: str,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3331."""
        broker = self.get_broker(servers=settings.url)
        async with self.patch_broker(broker) as br:
            await br.object_storage(queue)

        with pytest.raises(IncorrectState):
            await broker.object_storage(queue)
