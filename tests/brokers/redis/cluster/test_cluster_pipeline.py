from importlib.metadata import version
from typing import Literal
from unittest.mock import AsyncMock

import pytest
from redis.asyncio.cluster import RedisCluster
from redis.exceptions import RedisClusterException

from faststream.redis import ListSub, RedisClusterBroker
from faststream.redis.configs.state import RedisClusterConnectionState
from tests.brokers.redis.basic import RedisClusterTestcaseConfig


@pytest.mark.redis_cluster()
@pytest.mark.asyncio()
@pytest.mark.parametrize("kind", ("list", "stream", "batch"))
async def test_pipeline_preserves_queued_commands(
    monkeypatch: pytest.MonkeyPatch,
    queue: str,
    kind: Literal["list", "stream", "batch"],
) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/3044.

    Queue through the public broker without awaiting or executing the pipeline.
    """
    execute = AsyncMock(return_value=1)
    monkeypatch.setattr(RedisCluster, "execute_command", execute)
    monkeypatch.setattr(RedisCluster, "initialize", AsyncMock())
    broker = RedisClusterBroker()
    destination = f"{{{queue}}}:messages"
    publisher = broker.publisher(
        **(
            {"list": ListSub(destination, batch=True)}
            if kind == "batch"
            else {
                kind: destination,
            }
        )
    )

    pipe = RedisCluster(host="localhost").pipeline()
    pipe.incr(f"{{{queue}}}:count")

    if kind == "batch":
        result = await broker.publish_batch("one", "two", list=destination, pipeline=pipe)
        publisher_result = await publisher.publish("three", "four", pipeline=pipe)
    else:
        result = await broker.publish("one", **{kind: destination}, pipeline=pipe)
        publisher_result = await publisher.publish("two", pipeline=pipe)

    assert result is pipe
    assert publisher_result is pipe
    assert len(pipe) == 3
    execute.assert_not_awaited()


@pytest.mark.redis_cluster()
@pytest.mark.asyncio()
async def test_channel_pipeline_keeps_driver_restriction(
    monkeypatch: pytest.MonkeyPatch,
    queue: str,
) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/3044.

    Unsupported commands must not bypass the pipeline and publish immediately.
    """
    publish = AsyncMock()
    monkeypatch.setattr(RedisClusterConnectionState, "sync_publish", publish)
    broker = RedisClusterBroker()
    publisher = broker.publisher(channel=queue)

    pipe = RedisCluster(host="localhost").pipeline()
    pipe.incr(f"{{{queue}}}:count")

    with pytest.raises(RedisClusterException, match=r"publish.*blocked"):
        await broker.publish("one", channel=queue, pipeline=pipe)
    with pytest.raises(RedisClusterException, match=r"publish.*blocked"):
        await publisher.publish("two", pipeline=pipe)

    assert len(pipe) == 1
    publish.assert_not_awaited()


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
class TestClusterPipeline(RedisClusterTestcaseConfig):
    async def test_stream_transaction(self, queue: str) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3044.

        State updates and stream publications stay in the same-slot transaction.
        """
        broker = self.get_broker()
        counter = f"{{{queue}}}:count"
        stream = f"{{{queue}}}:stream"
        publisher = broker.publisher(stream=stream)

        async with broker:
            client = await broker.connect()
            try:
                async with client.pipeline(transaction=True) as pipe:
                    pipe.incr(counter)
                    await broker.publish(
                        "one",
                        stream=stream,
                        correlation_id=queue,
                        headers={"source": "broker"},
                        pipeline=pipe,
                    )
                    await publisher.publish("two", pipeline=pipe)

                    assert await client.exists(counter, stream) == 0
                    results = await pipe.execute()

                assert results[0] == 1
                assert len(results) == 3
                assert await client.get(counter) == b"1"
                entries = await client.xrange(stream)
                messages = [
                    broker.message_format.parse(fields[b"__data__"])
                    for _, fields in entries
                ]
                assert [body for body, _ in messages] == [b"one", b"two"]
                assert messages[0][1]["correlation_id"] == queue
                assert messages[0][1]["source"] == "broker"
            finally:
                await client.delete(counter, stream)

    async def test_list_batch_transaction(self, queue: str) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3044.

        Broker and batch-publisher calls append ordered messages at execution.
        """
        broker = self.get_broker()
        counter = f"{{{queue}}}:count"
        destination = f"{{{queue}}}:list"
        publisher = broker.publisher(list=ListSub(destination, batch=True))

        async with broker:
            client = await broker.connect()
            try:
                async with client.pipeline(transaction=True) as pipe:
                    pipe.incr(counter)
                    await broker.publish_batch(
                        "one", "two", list=destination, pipeline=pipe
                    )
                    await publisher.publish("three", "four", pipeline=pipe)

                    assert await client.exists(counter, destination) == 0
                    assert await pipe.execute() == [1, 2, 4]

                assert await client.get(counter) == b"1"
                messages = await client.lrange(destination, 0, -1)
                assert [broker.message_format.parse(msg)[0] for msg in messages] == [
                    b"one",
                    b"two",
                    b"three",
                    b"four",
                ]
            finally:
                await client.delete(counter, destination)

    async def test_watched_pipeline_before_multi(self, queue: str) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3044.

        Preserve redis-py's immediate commands between WATCH and MULTI.
        """
        broker = self.get_broker()
        counter = f"{{{queue}}}:count"
        stream = f"{{{queue}}}:stream"
        publisher = broker.publisher(stream=stream)

        async with broker:
            client = await broker.connect()
            try:
                async with client.pipeline(transaction=True) as pipe:
                    await pipe.watch(counter)
                    result = await broker.publish(
                        "immediate", stream=stream, pipeline=pipe
                    )
                    assert isinstance(result, bytes)
                    assert await client.xlen(stream) == 1

                    pipe.multi()
                    pipe.incr(counter)
                    await publisher.publish("queued", pipeline=pipe)
                    assert await client.get(counter) is None
                    assert await client.xlen(stream) == 1

                    results = await pipe.execute()

                assert results[0] == 1
                assert len(results) == 2
                assert await client.get(counter) == b"1"
                entries = await client.xrange(stream)
                assert [
                    broker.message_format.parse(fields[b"__data__"])[0]
                    for _, fields in entries
                ] == [b"immediate", b"queued"]
            finally:
                await client.delete(counter, stream)

    async def test_cross_slot_transaction_rejected(self, queue: str) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3044.

        A cross-slot transaction must fail without publishing outside it.
        """
        from redis.exceptions import CrossSlotTransactionError

        broker = self.get_broker()
        counter = f"{{first}}:{queue}:count"
        stream = f"{{second}}:{queue}:stream"

        async with broker:
            client = await broker.connect()
            try:
                async with client.pipeline(transaction=True) as pipe:
                    pipe.incr(counter)
                    await broker.publish("one", stream=stream, pipeline=pipe)
                    with pytest.raises(CrossSlotTransactionError):
                        await pipe.execute()

                assert await client.get(counter) is None
                assert await client.xlen(stream) == 0
            finally:
                await client.delete(counter)
                await client.delete(stream)
