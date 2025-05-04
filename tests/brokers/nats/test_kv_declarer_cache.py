from unittest.mock import patch

import pytest
from nats.js import JetStreamContext

from faststream.nats import NatsBroker
from tests.tools import spy_decorator


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_kv_storage_cache() -> None:
    broker = NatsBroker()
    await broker.connect()
    with patch.object(
        JetStreamContext,
        "create_key_value",
        spy_decorator(JetStreamContext.create_key_value),
    ) as m:
        await broker.key_value("test")
        await broker.key_value("test")
        assert broker._kv_declarer.buckets["test"]
        m.mock.assert_called_once()
