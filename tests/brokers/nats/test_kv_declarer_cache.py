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
class TestKVStorageCache(NatsTestcaseConfig):
    async def test_kv_storage_cache(self, settings: Settings, queue: str) -> None:
        broker = self.get_broker(servers=settings.url)
        async with self.patch_broker(broker) as br:
            with patch.object(
                JetStreamContext,
                "create_key_value",
                spy_decorator(JetStreamContext.create_key_value),
            ) as m:
                await br.key_value(queue)
                await br.key_value(queue)
                assert br.config.kv_declarer.buckets[queue]
                m.mock.assert_called_once()

    async def test_kv_storage_reconnect(self, settings: Settings, queue: str) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3331."""
        broker = self.get_broker(servers=settings.url)
        async with self.patch_broker(broker) as br:
            bucket = await br.key_value(queue)
            await bucket.put("key", b"original")

        async with self.patch_broker(broker) as br:
            bucket = await br.key_value(queue)
            assert (await bucket.get("key")).value == b"original"
            await bucket.put("key", b"updated")
            assert (await bucket.get("key")).value == b"updated"

    async def test_kv_storage_reconnect_without_declaration(
        self,
        settings: Settings,
        queue: str,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3331."""
        broker = self.get_broker(servers=settings.url)
        async with self.patch_broker(broker) as br:
            await br.key_value(queue)

        async with self.patch_broker(broker) as br:
            bucket = await br.key_value(queue, declare=False)
            await bucket.put("key", b"reconnected")
            assert (await bucket.get("key")).value == b"reconnected"

    async def test_kv_storage_access_after_disconnect(
        self, settings: Settings, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3331."""
        broker = self.get_broker(servers=settings.url)
        async with self.patch_broker(broker) as br:
            await br.key_value(queue)

        with pytest.raises(IncorrectState):
            await broker.key_value(queue)
