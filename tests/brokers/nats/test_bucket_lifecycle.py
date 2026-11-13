from typing import Literal

import pytest
from nats.js.kv import KeyValue
from nats.js.object_store import ObjectStore

from faststream.exceptions import IncorrectState
from faststream.nats import NatsBroker
from tests.brokers.nats.settings import Settings

pytestmark = [pytest.mark.asyncio(), pytest.mark.connected(), pytest.mark.nats()]


async def read_value(bucket: KeyValue | ObjectStore) -> bytes | None:
    if isinstance(bucket, KeyValue):
        return (await bucket.get("key")).value
    return (await bucket.get("key")).data


@pytest.mark.parametrize("storage", ("key_value", "object_storage"))
@pytest.mark.parametrize("declare", (True, False))
async def test_bucket_reconnect(
    settings: Settings,
    queue: str,
    storage: Literal["key_value", "object_storage"],
    declare: bool,
) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/3331."""
    broker = NatsBroker(settings.url)
    create_bucket = getattr(broker, storage)

    async with broker:
        previous = await create_bucket(queue)
        await previous.put("key", b"original")
        assert await create_bucket(queue) is previous

    for value in (b"first restart", b"second restart"):
        async with broker:
            current = await create_bucket(queue, declare=declare)
            await current.put("probe", value)
            assert await read_value(current) == b"original"
            await current.put("key", value)
            assert await read_value(current) == value
            assert current is not previous
            assert await create_bucket(queue, declare=declare) is current
            await current.put("key", b"original")
            previous = current

    with pytest.raises(IncorrectState):
        await create_bucket(queue)

    # Repeated stops remain safe, and a fresh connection can acquire the bucket.
    await broker.stop()
    async with broker:
        bucket = await create_bucket(queue)
        await bucket.put("key", b"recovered")
        assert await read_value(bucket) == b"recovered"
