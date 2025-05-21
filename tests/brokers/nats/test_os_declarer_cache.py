from unittest.mock import patch

import pytest
from nats.js import JetStreamContext

from faststream.nats import NatsBroker
from tests.tools import spy_decorator


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_object_storage_cache() -> None:
    broker = NatsBroker()
    await broker.connect()

    with patch.object(
        JetStreamContext,
        "create_object_store",
        spy_decorator(JetStreamContext.create_object_store),
    ) as m:
        await broker.object_storage("test")
        await broker.object_storage("test")
        assert broker._os_declarer.buckets["test"]
        m.mock.assert_called_once()
