from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from faststream.redis import (
    Redis as AnnotatedRedis,
    RedisBroker,
    RedisStreamMessage as AnnotatedRedisStreamMessage,
    StreamSub,
    TestRedisBroker,
)
from faststream.redis.message import DefaultStreamMessage, RedisStreamMessage

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.redis,
]


def make_message(
    *,
    stream: str = "orders",
    message_ids: list[bytes] | None = None,
) -> RedisStreamMessage:
    ids = [b"123-0"] if message_ids is None else message_ids
    raw_message = DefaultStreamMessage(
        type="stream",
        channel=stream,
        message_ids=ids,
        data={},
    )
    return RedisStreamMessage(
        raw_message=raw_message,
        body=b"",
    )


async def test_get_delivery_count_queries_exact_message() -> None:
    client = AsyncMock(spec=Redis)
    client.xpending_range = AsyncMock(return_value=[{"times_delivered": 3}])

    message = make_message()

    assert await message.get_delivery_count(client, "workers") == 3
    client.xpending_range.assert_awaited_once_with(
        name="orders",
        groupname="workers",
        min=b"123-0",
        max=b"123-0",
        count=1,
    )


async def test_get_delivery_count_supports_empty_stream_name() -> None:
    client = AsyncMock(spec=Redis)
    client.xpending_range = AsyncMock(return_value=[{"times_delivered": 2}])

    message = make_message(stream="")

    assert await message.get_delivery_count(client, "workers") == 2
    client.xpending_range.assert_awaited_once_with(
        name="",
        groupname="workers",
        min=b"123-0",
        max=b"123-0",
        count=1,
    )


async def test_get_delivery_count_defaults_when_message_is_not_pending() -> None:
    client = AsyncMock(spec=Redis)
    client.xpending_range = AsyncMock(return_value=[])

    message = make_message()

    assert await message.get_delivery_count(client, "workers") == 1


async def test_get_delivery_count_defaults_without_message_id() -> None:
    client = AsyncMock(spec=Redis)
    message = make_message(message_ids=[])

    assert await message.get_delivery_count(client, "workers") == 1
    client.xpending_range.assert_not_called()


async def test_get_delivery_count_propagates_redis_errors() -> None:
    client = AsyncMock(spec=Redis)
    client.xpending_range = AsyncMock(
        side_effect=RedisConnectionError("Redis unavailable")
    )

    message = make_message()

    with pytest.raises(RedisConnectionError, match="Redis unavailable"):
        await message.get_delivery_count(client, "workers")


async def test_test_broker_message_defaults_without_redis_id() -> None:
    broker = RedisBroker()
    counts: list[int] = []

    @broker.subscriber(stream=StreamSub("orders", group="workers", consumer="worker-1"))
    async def handler(
        message: AnnotatedRedisStreamMessage,
        redis: AnnotatedRedis,
    ) -> None:
        counts.append(await message.get_delivery_count(redis, "workers"))

    async with TestRedisBroker(broker) as test_broker:
        await test_broker.publish("hello", stream="orders")

    assert counts == [1]


@pytest.mark.connected()
async def test_delivery_count_for_new_message(queue: str) -> None:
    group = "workers"
    broker = RedisBroker()
    subscriber = broker.subscriber(
        stream=StreamSub(queue, group=group, consumer="worker-1")
    )

    async with broker:
        await broker.start()
        await broker.publish("hello", stream=queue)

        message = await subscriber.get_one(timeout=3)

        assert message is not None
        assert await message.get_delivery_count(broker._connection, group) == 1


@pytest.mark.connected()
async def test_delivery_count_after_xautoclaim_and_ack(queue: str) -> None:
    group = "workers"
    broker = RedisBroker()

    async with broker:
        await broker.start()
        await broker.publish("hello", stream=queue)
        await broker._connection.xgroup_create(queue, group, id="0")
        await broker._connection.xreadgroup(
            groupname=group,
            consumername="worker-1",
            streams={queue: ">"},
            count=1,
        )

        subscriber = broker.subscriber(
            stream=StreamSub(
                queue,
                group=group,
                consumer="worker-2",
                min_idle_time=0,
            )
        )
        message = await subscriber.get_one(timeout=3)

        assert message is not None
        assert await message.get_delivery_count(broker._connection, group) == 2

        await message.ack(redis=broker._connection, group=group)
        assert await message.get_delivery_count(broker._connection, group) == 1
