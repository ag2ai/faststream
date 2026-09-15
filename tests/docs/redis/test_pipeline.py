from importlib.metadata import version

import pytest

from faststream.redis import RedisClusterBroker, TestApp, TestRedisBroker
from tests.brokers.redis.cluster.settings import SettingsCluster


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_pipeline() -> None:
    from docs.docs_src.redis.pipeline.pipeline import (
        app,
        broker,
        handle,
    )

    broker.config.fd_config.serializer = None
    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("Hi!")


@pytest.mark.connected()
@pytest.mark.redis_cluster()
@pytest.mark.asyncio()
@pytest.mark.skipif(
    tuple(map(int, version("redis").split(".")[:2])) < (6, 2),
    reason=(
        "https://github.com/ag2ai/faststream/issues/3044: "
        "Redis Cluster transactions require redis-py >=6.2."
    ),
)
async def test_cluster_pipeline(queue: str) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/3044."""
    from docs.docs_src.redis.pipeline.cluster_pipeline import increment_and_publish

    broker = RedisClusterBroker(SettingsCluster().url)
    counter = f"{{{queue}}}:count"
    stream = f"{{{queue}}}:events"

    async with broker:
        client = await broker.connect()
        try:
            results = await increment_and_publish(broker, key=queue)

            assert results[0] == 1
            assert await client.get(counter) == b"1"
            entries = await client.xrange(stream)
            assert len(entries) == 1
            entry_id, fields = entries[0]
            assert results == [1, entry_id]
            assert broker.message_format.parse(fields[b"__data__"])[0] == b"created"
        finally:
            await client.delete(counter, stream)
