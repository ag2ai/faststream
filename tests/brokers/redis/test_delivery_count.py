from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from faststream.redis import (
    RedisBroker,
    RedisStreamMessage as AnnotatedRedisStreamMessage,
    StreamSub,
    TestRedisBroker,
)
from faststream.redis.message import DefaultStreamMessage, RedisStreamMessage
from faststream.redis.parser import BinaryMessageFormatV1

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.redis,
]


def make_message(
    redis: Callable[[], Redis[bytes]] | None,
    *,
    group: str | None = "workers",
    message_ids: list[bytes] | None = None,
) -> RedisStreamMessage:
    raw_message = DefaultStreamMessage(
        type="stream",
        channel="orders",
        message_ids=[b"123-0"] if message_ids is None else message_ids,
        data={},
    )
    message = RedisStreamMessage(
        raw_message=raw_message,
        body=b"",
    )
    message._set_delivery_count_context(redis=redis, group=group)
    return message


async def test_get_delivery_count_queries_exact_message() -> None:
    client = AsyncMock(spec=Redis)
    client.xpending_range = AsyncMock(return_value=[{"times_delivered": 3}])
    get_client = MagicMock(return_value=client)

    message = make_message(get_client)

    assert await message.get_delivery_count() == 3
    get_client.assert_called_once_with()
    client.xpending_range.assert_awaited_once_with(
        name="orders",
        groupname="workers",
        min=b"123-0",
        max=b"123-0",
        count=1,
    )


async def test_get_delivery_count_defaults_when_message_is_not_pending() -> None:
    client = AsyncMock(spec=Redis)
    client.xpending_range = AsyncMock(return_value=[])

    message = make_message(lambda: client)

    assert await message.get_delivery_count() == 1


@pytest.mark.parametrize(
    ("redis", "group", "message_ids"),
    (
        pytest.param(None, "workers", [b"123-0"], id="no-client"),
        pytest.param(MagicMock(), None, [b"123-0"], id="no-group"),
        pytest.param(MagicMock(), "workers", [], id="no-message-id"),
    ),
)
async def test_get_delivery_count_defaults_without_pending_context(
    redis: Callable[[], Redis[bytes]] | None,
    group: str | None,
    message_ids: list[bytes],
) -> None:
    message = make_message(redis, group=group, message_ids=message_ids)

    assert await message.get_delivery_count() == 1
    if redis is not None:
        redis.assert_not_called()  # type: ignore[attr-defined]


async def test_get_delivery_count_propagates_redis_errors() -> None:
    client = AsyncMock(spec=Redis)
    client.xpending_range = AsyncMock(
        side_effect=RedisConnectionError("Redis unavailable")
    )

    message = make_message(lambda: client)

    with pytest.raises(RedisConnectionError, match="Redis unavailable"):
        await message.get_delivery_count()


async def test_stream_subscriber_uses_current_client_for_delivery_count() -> None:
    original_client = AsyncMock(spec=Redis)
    original_client.xpending_range = AsyncMock()
    current_client = AsyncMock(spec=Redis)
    current_client.xpending_range = AsyncMock(return_value=[{"times_delivered": 2}])
    raw_data = await BinaryMessageFormatV1.encode(
        message="hello",
        reply_to=None,
        headers=None,
        correlation_id="correlation-id",
    )
    broker = RedisBroker()
    subscriber = broker.subscriber(
        stream=StreamSub("orders", group="workers", consumer="worker-1")
    )
    broker.config.broker_config.connection._client = original_client
    parser, _ = subscriber._get_parser_and_decoder()

    message = await parser(
        DefaultStreamMessage(
            type="stream",
            channel="orders",
            message_ids=[b"123-0"],
            data={b"__data__": raw_data},
        )
    )

    broker.config.broker_config.connection._client = current_client

    assert isinstance(message, RedisStreamMessage)
    assert await message.get_delivery_count() == 2
    original_client.xpending_range.assert_not_awaited()
    current_client.xpending_range.assert_awaited_once()


@pytest.mark.parametrize("parser_scope", ("broker", "subscriber", "handler"))
async def test_custom_stream_parser_preserves_delivery_count_context(
    parser_scope: str,
) -> None:
    client = AsyncMock(spec=Redis)
    client.xpending_range = AsyncMock(return_value=[{"times_delivered": 7}])

    async def custom_parser(message: DefaultStreamMessage) -> RedisStreamMessage:
        return RedisStreamMessage(raw_message=message, body=b"")

    broker = RedisBroker(
        parser=custom_parser if parser_scope == "broker" else None,
    )
    subscriber = broker.subscriber(
        stream=StreamSub("orders", group="workers", consumer="worker-1"),
        parser=custom_parser if parser_scope == "subscriber" else None,
    )
    broker.config.broker_config.connection._client = client
    parser, _ = subscriber._get_parser_and_decoder(
        custom_parser if parser_scope == "handler" else None
    )

    message = await parser(
        DefaultStreamMessage(
            type="stream",
            channel="orders",
            message_ids=[b"123-0"],
            data={},
        )
    )

    assert isinstance(message, RedisStreamMessage)
    assert await message.get_delivery_count() == 7


async def test_test_broker_message_defaults_without_redis_id() -> None:
    broker = RedisBroker()
    counts: list[int] = []

    @broker.subscriber(stream=StreamSub("orders", group="workers", consumer="worker-1"))
    async def handler(message: AnnotatedRedisStreamMessage) -> None:
        counts.append(await message.get_delivery_count())

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
        assert await message.get_delivery_count() == 1


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
        assert await message.get_delivery_count() == 2

        await message.ack(redis=broker._connection, group=group)
        assert await message.get_delivery_count() == 1
