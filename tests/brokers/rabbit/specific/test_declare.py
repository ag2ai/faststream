from typing import TYPE_CHECKING, Any, Optional
from unittest.mock import AsyncMock

import pytest

from faststream.exceptions import SetupError
from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)
from faststream.rabbit.helpers.declarer import RabbitDeclarerImpl

if TYPE_CHECKING:
    import aio_pika

    from faststream.rabbit.schemas import Channel


class FakeChannelManager:
    def __init__(self, async_mock: AsyncMock) -> None:
        self.async_mock = async_mock

    async def get_channel(
        self,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustChannel":
        return self.async_mock


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_declare_queue(async_mock: AsyncMock, queue: str) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))

    q1 = await declarer.declare_queue(RabbitQueue(queue))
    q2 = await declarer.declare_queue(RabbitQueue(queue))

    assert q1 is q2
    async_mock.declare_queue.assert_awaited_once()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_declare_exchange(async_mock: AsyncMock, queue: str) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))

    ex1 = await declarer.declare_exchange(RabbitExchange(queue))
    ex2 = await declarer.declare_exchange(RabbitExchange(queue))

    assert ex1 is ex2
    async_mock.declare_exchange.assert_awaited_once()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ("settings", "conflicting_field"),
    (
        pytest.param({"durable": False}, "durable", id="durable"),
        pytest.param({"exclusive": True}, "exclusive", id="exclusive"),
        pytest.param({"auto_delete": True}, "auto_delete", id="auto-delete"),
        pytest.param(
            {"arguments": {"custom": ["value"]}},
            "arguments",
            id="arguments",
        ),
    ),
)
async def test_reject_conflicting_queue_declaration(
    async_mock: AsyncMock,
    queue: str,
    settings: dict[str, Any],
    conflicting_field: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))
    await declarer.declare_queue(RabbitQueue(queue))

    with pytest.raises(
        SetupError,
        match=rf"RabbitQueue .*{conflicting_field}",
    ):
        await declarer.declare_queue(RabbitQueue(queue, **settings))

    async_mock.declare_queue.assert_awaited_once()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ("settings", "conflicting_field"),
    (
        pytest.param(
            {"type": ExchangeType.FANOUT},
            "type",
            id="type",
        ),
        pytest.param({"durable": False}, "durable", id="durable"),
        pytest.param({"auto_delete": True}, "auto_delete", id="auto-delete"),
        pytest.param(
            {"arguments": {"custom": ["value"]}},
            "arguments",
            id="arguments",
        ),
    ),
)
async def test_reject_conflicting_exchange_declaration(
    async_mock: AsyncMock,
    queue: str,
    settings: dict[str, Any],
    conflicting_field: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))
    await declarer.declare_exchange(RabbitExchange(queue))

    with pytest.raises(
        SetupError,
        match=rf"RabbitExchange .*{conflicting_field}",
    ):
        await declarer.declare_exchange(RabbitExchange(queue, **settings))

    async_mock.declare_exchange.assert_awaited_once()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_passive_queue_access_ignores_declaration_settings(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))
    q1 = await declarer.declare_queue(RabbitQueue(queue))
    q2 = await declarer.declare_queue(
        RabbitQueue(queue, durable=False),
        declare=False,
    )

    assert q1 is q2
    async_mock.declare_queue.assert_awaited_once()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_passive_exchange_access_ignores_declaration_settings(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))
    ex1 = await declarer.declare_exchange(
        RabbitExchange(queue, type=ExchangeType.TOPIC),
    )
    ex2 = await declarer.declare_exchange(
        RabbitExchange(queue),
        declare=False,
    )

    assert ex1 is ex2
    async_mock.declare_exchange.assert_awaited_once()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_active_queue_replaces_passive_cache_entry(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    passive_object = object()
    active_object = object()
    async_mock.declare_queue.side_effect = (passive_object, active_object)
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))

    q1 = await declarer.declare_queue(RabbitQueue(queue), declare=False)
    active_queue = RabbitQueue(queue, durable=False)
    q2 = await declarer.declare_queue(active_queue)
    q3 = await declarer.declare_queue(active_queue)

    assert q1 is passive_object
    assert q2 is active_object
    assert q3 is active_object
    assert async_mock.declare_queue.await_count == 2
    assert async_mock.declare_queue.await_args_list[0].kwargs["passive"] is True
    assert async_mock.declare_queue.await_args_list[1].kwargs["passive"] is False


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_active_exchange_replaces_passive_cache_entry(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    passive_object = object()
    active_object = object()
    async_mock.declare_exchange.side_effect = (passive_object, active_object)
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))

    ex1 = await declarer.declare_exchange(RabbitExchange(queue), declare=False)
    active_exchange = RabbitExchange(queue, type=ExchangeType.TOPIC)
    ex2 = await declarer.declare_exchange(active_exchange)
    ex3 = await declarer.declare_exchange(active_exchange)

    assert ex1 is passive_object
    assert ex2 is active_object
    assert ex3 is active_object
    assert async_mock.declare_exchange.await_count == 2
    assert async_mock.declare_exchange.await_args_list[0].kwargs["passive"] is True
    assert async_mock.declare_exchange.await_args_list[1].kwargs["passive"] is False


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_queue_binding_settings_do_not_conflict(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))
    q1 = await declarer.declare_queue(
        RabbitQueue(
            queue,
            routing_key="first",
            bind_arguments={"key": "first"},
        ),
    )
    q2 = await declarer.declare_queue(
        RabbitQueue(
            queue,
            routing_key="second",
            bind_arguments={"key": "second"},
        ),
    )

    assert q1 is q2
    async_mock.declare_queue.assert_awaited_once()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_exchange_binding_settings_do_not_conflict(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))
    ex1 = await declarer.declare_exchange(
        RabbitExchange(
            queue,
            bind_to=RabbitExchange(f"{queue}-parent-1"),
            routing_key="first",
            bind_arguments={"key": "first"},
        ),
    )
    ex2 = await declarer.declare_exchange(
        RabbitExchange(
            queue,
            bind_to=RabbitExchange(f"{queue}-parent-2"),
            routing_key="second",
            bind_arguments={"key": "second"},
        ),
    )

    assert ex1 is ex2
    assert async_mock.declare_exchange.await_count == 2


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_reuse_declarations_with_nested_arguments(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))
    arguments = {"custom": ["value", {"nested": True}]}

    q1 = await declarer.declare_queue(RabbitQueue(queue, arguments=arguments))
    q2 = await declarer.declare_queue(RabbitQueue(queue, arguments=arguments))
    ex1 = await declarer.declare_exchange(
        RabbitExchange(f"{queue}-exchange", arguments=arguments),
    )
    ex2 = await declarer.declare_exchange(
        RabbitExchange(f"{queue}-exchange", arguments=arguments),
    )

    assert q1 is q2
    assert ex1 is ex2
    async_mock.declare_queue.assert_awaited_once()
    async_mock.declare_exchange.assert_awaited_once()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_declare_nested_exchange_cash_nested(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))

    exchange = RabbitExchange(queue)

    await declarer.declare_exchange(RabbitExchange(queue + "1", bind_to=exchange))
    assert async_mock.declare_exchange.await_count == 2

    await declarer.declare_exchange(exchange)
    assert async_mock.declare_exchange.await_count == 2


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_reject_conflicting_cached_parent_exchange(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))
    parent = RabbitExchange(f"{queue}-parent")
    await declarer.declare_exchange(RabbitExchange(queue, bind_to=parent))

    with pytest.raises(SetupError, match=r"RabbitExchange .*durable"):
        await declarer.declare_exchange(
            RabbitExchange(parent.name, durable=False),
        )

    assert async_mock.declare_exchange.await_count == 2


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_detect_queue_schema_mutation(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))
    arguments: dict[str, Any] = {"custom": ["first"]}
    schema = RabbitQueue(queue, arguments=arguments)
    await declarer.declare_queue(schema)
    declared_arguments = async_mock.declare_queue.await_args.kwargs["arguments"]

    arguments["custom"].append("second")

    assert declared_arguments == {
        "x-queue-type": "classic",
        "custom": ["first"],
    }
    with pytest.raises(SetupError, match=r"RabbitQueue .*arguments"):
        await declarer.declare_queue(schema)

    async_mock.declare_queue.assert_awaited_once()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_detect_exchange_schema_mutation(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))
    arguments: dict[str, Any] = {"custom": ["first"]}
    schema = RabbitExchange(queue, arguments=arguments)
    await declarer.declare_exchange(schema)
    declared_arguments = async_mock.declare_exchange.await_args.kwargs["arguments"]

    arguments["custom"].append("second")

    assert declared_arguments == {"custom": ["first"]}
    with pytest.raises(SetupError, match=r"RabbitExchange .*arguments"):
        await declarer.declare_exchange(schema)

    async_mock.declare_exchange.assert_awaited_once()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_disconnect_clears_declaration_settings(
    async_mock: AsyncMock,
    queue: str,
) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))
    await declarer.declare_queue(RabbitQueue(queue))

    with pytest.raises(SetupError):
        await declarer.declare_queue(RabbitQueue(queue, durable=False))

    declarer.disconnect()
    await declarer.declare_queue(RabbitQueue(queue, durable=False))

    assert async_mock.declare_queue.await_count == 2


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_publisher_declare(async_mock: AsyncMock, queue: str) -> None:
    declarer = RabbitDeclarerImpl(FakeChannelManager(async_mock))

    broker = RabbitBroker()
    broker._connection = async_mock
    broker.config.declarer = declarer

    @broker.publisher(queue, queue)
    async def f() -> None: ...

    await broker.start()

    async_mock.declare_queue.assert_awaited_once()
    assert async_mock.declare_queue.await_args.kwargs["name"] == "amq.rabbitmq.reply-to"
    async_mock.declare_exchange.assert_awaited_once()
    assert async_mock.declare_exchange.await_args.kwargs["name"] == queue
