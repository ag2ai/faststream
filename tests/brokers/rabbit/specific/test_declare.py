from asyncio import CancelledError
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock, sentinel

import pytest
from aio_pika import Message

from faststream.exceptions import SetupError
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.helpers.declarer import RabbitDeclarerImpl
from tests.brokers.rabbit.basic import RabbitTestcaseConfig

pytestmark = [pytest.mark.rabbit(), pytest.mark.asyncio()]


class TestQueueDeclarationCache:
    async def test_equivalent_settings_reuse_resource(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        arguments: Any = {"custom": ["value", {"nested": True}]}
        cached = await declarer.declare_queue(RabbitQueue(queue, arguments=arguments))
        equivalent = RabbitQueue(
            queue,
            arguments=deepcopy(arguments),
            bind_arguments={"header": "value"},
            timeout=1,
            robust=False,
        )
        async_mock.declare_queue.reset_mock()

        result = await declarer.declare_queue(equivalent)

        assert result is cached
        async_mock.declare_queue.assert_not_awaited()

    @pytest.mark.parametrize(
        ("field", "value"),
        (
            pytest.param("durable", False, id="durable"),
            pytest.param("exclusive", True, id="exclusive"),
            pytest.param("auto_delete", True, id="auto-delete"),
            pytest.param(
                "arguments",
                {"custom": ["changed", {"nested": False}]},
                id="nested-arguments",
            ),
        ),
    )
    async def test_conflicting_settings_fail_before_reaching_driver(
        self, async_mock: AsyncMock, queue: str, field: str, value: Any
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        arguments: Any = {"custom": ["value", {"nested": True}]}
        await declarer.declare_queue(RabbitQueue(queue, arguments=arguments))
        conflicting = RabbitQueue(queue, **{"arguments": arguments, field: value})
        async_mock.declare_queue.reset_mock()

        with pytest.raises(SetupError, match=f"RabbitQueue .*{field}"):
            await declarer.declare_queue(conflicting)

        async_mock.declare_queue.assert_not_awaited()

    async def test_disconnect_allows_new_settings(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        async_mock.declare_queue.side_effect = (sentinel.cached, sentinel.replacement)
        await declarer.declare_queue(RabbitQueue(queue))
        replacement = RabbitQueue(queue, durable=False)
        async_mock.declare_queue.reset_mock()

        declarer.disconnect()
        result = await declarer.declare_queue(replacement)

        assert result is sentinel.replacement
        async_mock.declare_queue.assert_awaited_once()

    async def test_unnamed_queues_are_declared_independently(
        self, async_mock: AsyncMock
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        async_mock.declare_queue.side_effect = (sentinel.first, sentinel.second)
        first = RabbitQueue("", durable=False, exclusive=True)
        second = RabbitQueue("", durable=False)

        results = (
            await declarer.declare_queue(first),
            await declarer.declare_queue(second),
        )

        assert results == (sentinel.first, sentinel.second)


class TestExchangeDeclarationCache:
    async def test_equivalent_settings_reuse_resource(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        arguments: Any = {"custom": ["value", {"nested": True}]}
        cached = await declarer.declare_exchange(
            RabbitExchange(queue, arguments=arguments)
        )
        equivalent = RabbitExchange(
            queue,
            arguments=deepcopy(arguments),
            bind_arguments={"header": "value"},
            timeout=1,
            robust=False,
        )
        async_mock.declare_exchange.reset_mock()

        result = await declarer.declare_exchange(equivalent)

        assert result is cached
        async_mock.declare_exchange.assert_not_awaited()

    @pytest.mark.parametrize(
        ("field", "value"),
        (
            pytest.param("type", ExchangeType.TOPIC, id="type"),
            pytest.param("durable", False, id="durable"),
            pytest.param("auto_delete", True, id="auto-delete"),
            pytest.param(
                "arguments",
                {"custom": ["changed", {"nested": False}]},
                id="nested-arguments",
            ),
        ),
    )
    async def test_conflicting_settings_fail_before_reaching_driver(
        self, async_mock: AsyncMock, queue: str, field: str, value: Any
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        arguments: Any = {"custom": ["value", {"nested": True}]}
        await declarer.declare_exchange(RabbitExchange(queue, arguments=arguments))
        conflicting = RabbitExchange(queue, **{"arguments": arguments, field: value})
        async_mock.declare_exchange.reset_mock()

        with pytest.raises(SetupError, match=f"RabbitExchange .*{field}"):
            await declarer.declare_exchange(conflicting)

        async_mock.declare_exchange.assert_not_awaited()

    async def test_disconnect_allows_new_settings(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        async_mock.declare_exchange.side_effect = (sentinel.cached, sentinel.replacement)
        await declarer.declare_exchange(RabbitExchange(queue))
        replacement = RabbitExchange(queue, durable=False)
        async_mock.declare_exchange.reset_mock()

        declarer.disconnect()
        result = await declarer.declare_exchange(replacement)

        assert result is sentinel.replacement
        async_mock.declare_exchange.assert_awaited_once()

    async def test_default_exchange_comes_from_channel(
        self, async_mock: AsyncMock
    ) -> None:
        """Use the channel's default exchange without declaring another exchange."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        async_mock.default_exchange = sentinel.default_exchange
        exchange = RabbitExchange()

        result = await declarer.declare_exchange(exchange)

        assert result is sentinel.default_exchange
        async_mock.declare_exchange.assert_not_awaited()


class TestQueuePassiveDeclarations:
    async def test_passive_access_ignores_conflicting_settings(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        cached = await declarer.declare_queue(RabbitQueue(queue))
        conflicting = RabbitQueue(queue, durable=False)
        async_mock.declare_queue.reset_mock()

        result = await declarer.declare_queue(conflicting, declare=False)

        assert result is cached
        async_mock.declare_queue.assert_not_awaited()

    async def test_matching_active_settings_reuse_passive_resource(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        cached = await declarer.declare_queue(RabbitQueue(queue, declare=False))
        matching = RabbitQueue(queue)

        result = await declarer.declare_queue(matching)

        assert result is cached
        async_mock.declare_queue.assert_awaited_once_with(
            name=queue,
            durable=True,
            exclusive=False,
            auto_delete=False,
            passive=True,
            arguments={"x-queue-type": "classic"},
            timeout=None,
            robust=True,
        )

    async def test_explicit_active_declaration_replaces_mismatching_passive_resource(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        async_mock.declare_queue.side_effect = (sentinel.passive, sentinel.active)
        await declarer.declare_queue(RabbitQueue(queue, declare=False))
        active = RabbitQueue(queue, durable=False, declare=False)
        async_mock.declare_queue.reset_mock()

        result = await declarer.declare_queue(active, declare=True)
        reused = await declarer.declare_queue(RabbitQueue(queue, durable=False))

        assert result is sentinel.active
        assert reused is result
        async_mock.declare_queue.assert_awaited_once_with(
            name=queue,
            durable=False,
            exclusive=False,
            auto_delete=False,
            passive=False,
            arguments={"x-queue-type": "classic"},
            timeout=None,
            robust=True,
        )


class TestExchangePassiveDeclarations:
    async def test_passive_access_ignores_conflicting_settings(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        cached = await declarer.declare_exchange(RabbitExchange(queue))
        conflicting = RabbitExchange(queue, durable=False)
        async_mock.declare_exchange.reset_mock()

        result = await declarer.declare_exchange(conflicting, declare=False)

        assert result is cached
        async_mock.declare_exchange.assert_not_awaited()

    async def test_matching_active_settings_reuse_passive_resource(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        cached = await declarer.declare_exchange(RabbitExchange(queue, declare=False))
        matching = RabbitExchange(queue)

        result = await declarer.declare_exchange(matching)

        assert result is cached
        async_mock.declare_exchange.assert_awaited_once_with(
            name=queue,
            type="direct",
            durable=True,
            auto_delete=False,
            passive=True,
            arguments={},
            timeout=None,
            robust=True,
            internal=False,
        )

    async def test_explicit_active_declaration_replaces_mismatching_passive_resource(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        async_mock.declare_exchange.side_effect = (sentinel.passive, sentinel.active)
        await declarer.declare_exchange(RabbitExchange(queue, declare=False))
        active = RabbitExchange(queue, durable=False, declare=False)
        async_mock.declare_exchange.reset_mock()

        result = await declarer.declare_exchange(active, declare=True)
        reused = await declarer.declare_exchange(RabbitExchange(queue, durable=False))

        assert result is sentinel.active
        assert reused is result
        async_mock.declare_exchange.assert_awaited_once_with(
            name=queue,
            type="direct",
            durable=False,
            auto_delete=False,
            passive=False,
            arguments={},
            timeout=None,
            robust=True,
            internal=False,
        )


class TestQueueDeclarationSnapshots:
    async def test_driver_arguments_cannot_rewrite_schema_or_cache(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        arguments: Any = {"custom": {"values": ["original"]}}
        schema = RabbitQueue(queue, arguments=arguments)

        def mutate_arguments(**kwargs: Any) -> object:
            kwargs["arguments"]["custom"]["values"].append("changed")
            return sentinel.queue

        async_mock.declare_queue.side_effect = mutate_arguments

        await declarer.declare_queue(schema)
        result = await declarer.declare_queue(RabbitQueue(queue, arguments=arguments))

        assert result is sentinel.queue
        assert schema.arguments == {
            "x-queue-type": "classic",
            "custom": {"values": ["original"]},
        }
        async_mock.declare_queue.assert_awaited_once()

    async def test_settings_are_captured_before_waiting_for_channel(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        arguments: Any = {"custom": {"values": ["original"]}}
        original: Any = {"custom": {"values": ["original"]}}
        schema = RabbitQueue(queue, arguments=arguments)

        def mutate_schema(channel: object) -> AsyncMock:
            schema.name = f"{queue}-changed"
            schema.durable = False
            schema.auto_delete = True
            schema.timeout = 1
            schema.robust = False
            schema.exclusive = True
            arguments["custom"]["values"].append("changed")
            return async_mock

        manager.get_channel.side_effect = mutate_schema

        await declarer.declare_queue(schema)
        await declarer.declare_queue(RabbitQueue(queue, arguments=original))

        async_mock.declare_queue.assert_awaited_once_with(
            name=queue,
            durable=True,
            exclusive=False,
            auto_delete=False,
            passive=False,
            arguments={
                "x-queue-type": "classic",
                "custom": {"values": ["original"]},
            },
            timeout=None,
            robust=True,
        )


class TestExchangeDeclarationSnapshots:
    async def test_driver_arguments_cannot_rewrite_schema_or_cache(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        arguments: Any = {"custom": {"values": ["original"]}}
        schema = RabbitExchange(queue, arguments=arguments)

        def mutate_arguments(**kwargs: Any) -> object:
            kwargs["arguments"]["custom"]["values"].append("changed")
            return sentinel.exchange

        async_mock.declare_exchange.side_effect = mutate_arguments

        await declarer.declare_exchange(schema)
        result = await declarer.declare_exchange(
            RabbitExchange(queue, arguments=arguments)
        )

        assert result is sentinel.exchange
        assert schema.arguments == {"custom": {"values": ["original"]}}
        async_mock.declare_exchange.assert_awaited_once()

    async def test_settings_are_captured_before_waiting_for_channel(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        arguments: Any = {"custom": {"values": ["original"]}}
        original: Any = {"custom": {"values": ["original"]}}
        schema = RabbitExchange(queue, arguments=arguments)

        def mutate_schema(channel: object) -> AsyncMock:
            schema.name = f"{queue}-changed"
            schema.durable = False
            schema.auto_delete = True
            schema.timeout = 1
            schema.robust = False
            schema.type = ExchangeType.TOPIC
            arguments["custom"]["values"].append("changed")
            return async_mock

        manager.get_channel.side_effect = mutate_schema

        await declarer.declare_exchange(schema)
        await declarer.declare_exchange(RabbitExchange(queue, arguments=original))

        async_mock.declare_exchange.assert_awaited_once_with(
            name=queue,
            type="direct",
            durable=True,
            auto_delete=False,
            passive=False,
            arguments={"custom": {"values": ["original"]}},
            timeout=None,
            robust=True,
            internal=False,
        )


class TestExchangeBindings:
    async def test_binding_reuses_recursively_declared_parent(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Reuse the parent exchange declared while binding its child."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        child, parent = AsyncMock(name="child"), AsyncMock(name="parent")
        async_mock.declare_exchange.side_effect = (child, parent)
        parent_schema = RabbitExchange(f"{queue}-parent")
        schema = RabbitExchange(
            queue,
            bind_to=parent_schema,
            routing_key="route",
            bind_arguments={"header": "value"},
            timeout=1,
            robust=False,
        )

        await declarer.declare_exchange(schema)
        cached_parent = await declarer.declare_exchange(
            RabbitExchange(parent_schema.name)
        )

        assert cached_parent is parent
        child.bind.assert_awaited_once_with(
            exchange=parent,
            routing_key="route",
            arguments={"header": "value"},
            timeout=1,
            robust=False,
        )

    @pytest.mark.parametrize(
        "failure",
        (
            pytest.param(RuntimeError, id="binding-error"),
            pytest.param(CancelledError, id="binding-cancelled"),
        ),
    )
    async def test_failed_binding_redeclares_child_and_reuses_parent(
        self, async_mock: AsyncMock, queue: str, failure: type[BaseException]
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        failed = AsyncMock(name="failed-child")
        parent = AsyncMock(name="parent")
        retried = AsyncMock(name="retried-child")
        failed.bind.side_effect = failure
        async_mock.declare_exchange.side_effect = (failed, parent, retried)
        schema = RabbitExchange(queue, bind_to=RabbitExchange(f"{queue}-parent"))
        with pytest.raises(failure):
            await declarer.declare_exchange(schema)
        async_mock.declare_exchange.reset_mock()

        await declarer.declare_exchange(schema)

        async_mock.declare_exchange.assert_awaited_once()
        retried.bind.assert_awaited_once_with(
            exchange=parent, routing_key=queue, arguments=None, timeout=None, robust=True
        )

    async def test_correcting_parent_conflict_retries_child_binding(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        parent = AsyncMock(name="parent")
        failed = AsyncMock(name="failed-child")
        retried = AsyncMock(name="retried-child")
        async_mock.declare_exchange.side_effect = (parent, failed, retried)
        parent_schema = RabbitExchange(f"{queue}-parent")
        await declarer.declare_exchange(parent_schema)
        with pytest.raises(SetupError, match="durable"):
            await declarer.declare_exchange(
                RabbitExchange(
                    queue, bind_to=RabbitExchange(parent_schema.name, durable=False)
                )
            )

        await declarer.declare_exchange(RabbitExchange(queue, bind_to=parent_schema))

        retried.bind.assert_awaited_once_with(
            exchange=parent, routing_key=queue, arguments=None, timeout=None, robust=True
        )

    async def test_failed_active_upgrade_preserves_passive_resource(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        manager: Any = AsyncMock()
        manager.get_channel.return_value = async_mock
        declarer = RabbitDeclarerImpl(manager)
        passive = AsyncMock(name="passive-exchange")
        active = AsyncMock(name="active-exchange")
        parent = AsyncMock(name="parent")
        async_mock.declare_exchange.side_effect = (passive, active, parent)
        await declarer.declare_exchange(RabbitExchange(queue, declare=False))
        active.bind.side_effect = RuntimeError
        replacement = RabbitExchange(
            queue, durable=False, bind_to=RabbitExchange(f"{queue}-parent")
        )

        with pytest.raises(RuntimeError):
            await declarer.declare_exchange(replacement)
        result = await declarer.declare_exchange(RabbitExchange(queue))

        assert result is passive


class TestPublisherDeclarations:
    async def test_start_declares_exchange_without_declaring_destination_queue(
        self, async_mock: AsyncMock, queue: str
    ) -> None:
        """Declare the publisher's exchange without declaring its destination queue."""
        declarer: Any = async_mock
        broker = RabbitBroker()
        broker.config.broker_config.declarer = declarer
        exchange = RabbitExchange(queue)
        publisher = broker.publisher(queue, exchange)

        await publisher.start()

        declarer.declare_queue.assert_not_awaited()
        declarer.declare_exchange.assert_awaited_once_with(exchange)


@pytest.mark.connected()
class TestRobustBindings(RabbitTestcaseConfig):
    async def test_matching_active_declaration_preserves_bindings_on_restore(
        self, settings: Any, queue: str
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2034."""
        broker = self.get_broker(url=settings.url)
        async with self.patch_broker(broker):
            connection = await broker.connect()
            channel = await connection.channel()
            existing = await channel.declare_exchange(queue, durable=True)
            parent = await channel.declare_exchange(f"{queue}-parent", durable=True)
            destination = await channel.declare_queue(exclusive=True)
            await destination.bind(existing, routing_key="retained")
            declarer = broker.config.broker_config.declarer
            passive = await declarer.declare_exchange(
                RabbitExchange(queue, declare=False)
            )
            await passive.bind(parent, routing_key="retained")
            underlay = await channel.get_underlay_channel()
            await underlay.exchange_unbind(
                destination=queue, source=parent.name, routing_key="retained"
            )

            active = await declarer.declare_exchange(RabbitExchange(queue))
            await active.restore()
            await parent.publish(Message(b"restored"), routing_key="retained")
            message = await destination.get(timeout=3, no_ack=True)

            assert message is not None
            assert message.body == b"restored"
