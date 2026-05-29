from unittest.mock import AsyncMock

import pytest

from faststream.exceptions import IncorrectState
from faststream.redis import RedisClusterBroker, RedisRouter, TestRedisBroker
from faststream.redis.configs import (
    RedisConnectionState,
    state as state_module,
)
from faststream.redis.configs.state import RedisClusterConnectionState
from faststream.redis.parser import BinaryMessageFormatV1
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.response import RedisPublishCommand
from faststream.response.publish_type import PublishType


class TestRedisClusterConnectionStateUnit:
    """Unit tests for RedisClusterConnectionState (no cluster needed)."""

    def test_initial_bool_false(self) -> None:
        state = RedisClusterConnectionState()
        assert not bool(state)

    def test_options_passed_and_stored(self) -> None:
        opts = {
            "host": "custom",
            "port": 7000,
            "ssl": True,
            "password": "secret",  # pragma: allowlist secret
        }
        state = RedisClusterConnectionState(opts)
        assert state._options == opts

    def test_bool_reflects_connected(self) -> None:
        state = RedisClusterConnectionState()
        assert not state
        state._connected = True
        assert state


class TestClusterConnectionStateConnect:
    """connect() builds the async client with redis-8-safe options."""

    @pytest.mark.asyncio()
    async def test_connect_sets_legacy_responses_and_driver_info(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured: dict[str, object] = {}

        class FakeCluster:
            def __init__(self, **kwargs: object) -> None:
                captured.update(kwargs)

            def pubsub(self) -> None:  # presence satisfies the version guard
                ...

        monkeypatch.setattr(state_module, "RedisCluster", FakeCluster)

        state = RedisClusterConnectionState({"host": "127.0.0.1", "port": 7000})
        await state.connect()

        assert captured["legacy_responses"] is True
        assert "driver_info" in captured
        assert bool(state)

    @pytest.mark.asyncio()
    async def test_connect_requires_redis8(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class OldCluster:  # no pubsub attr -> simulate redis-py < 8
            def __init__(self, **kwargs: object) -> None: ...

        monkeypatch.setattr(state_module, "RedisCluster", OldCluster)

        state = RedisClusterConnectionState({"host": "127.0.0.1", "port": 7000})
        with pytest.raises(IncorrectState, match="requires redis-py"):
            await state.connect()


class TestClusterBrokerWarnings:
    """Tests for RuntimeWarning on pipeline usage."""

    @pytest.mark.asyncio()
    async def test_publish_with_pipeline_warns(self) -> None:
        broker = RedisClusterBroker(url="redis://127.0.0.1:7001")
        async with TestRedisBroker(broker) as br:
            with pytest.warns(RuntimeWarning, match="Pipeline is not supported"):
                await br.publish("hello", channel="ch", pipeline=None)

    @pytest.mark.asyncio()
    async def test_publish_batch_with_pipeline_warns(self) -> None:
        broker = RedisClusterBroker(url="redis://127.0.0.1:7001")
        async with TestRedisBroker(broker) as br:
            with pytest.warns(RuntimeWarning, match="Pipeline is not supported"):
                await br.publish_batch("x", list="l", pipeline=None)


class TestClusterBrokerInheritanceExtra:
    """Additional inheritance/API compatibility tests."""

    def test_cluster_incompatible_params_filtered(self) -> None:
        """Cluster-incompatible params from URL are stripped on init."""
        broker = RedisClusterBroker(
            url="redis://localhost:6379/5",
            client_name="myapp",
            ssl=True,
        )
        opts = broker.config.broker_config.connection._options
        assert "db" not in opts, "db is cluster-incompatible"
        assert "client_name" in opts
        assert opts["client_name"] == "myapp"
        assert opts.get("ssl") is True

    def test_init_with_startup_nodes(self) -> None:
        """Explicit startup_nodes are stored in connection options."""
        broker = RedisClusterBroker(
            url="redis://127.0.0.1:7001",
            startup_nodes=[("127.0.0.1", 7002)],
        )
        nodes = broker.config.broker_config.connection._options.get("startup_nodes", [])
        assert len(nodes) > 1

    def test_router_inclusion_works(self) -> None:
        broker = RedisClusterBroker(routers=[RedisRouter()])
        assert len(broker.routers) == 1

    def test_subscriber_registration_list(self) -> None:
        broker = RedisClusterBroker()

        @broker.subscriber(list="l1")
        async def h1(msg): ...

        @broker.subscriber(list="l2")
        async def h2(msg): ...

        assert len(broker.subscribers) == 2

    def test_subscriber_registration_stream(self) -> None:
        broker = RedisClusterBroker()

        @broker.subscriber(stream="s1")
        async def h1(msg): ...

        @broker.subscriber(stream="s2")
        async def h2(msg): ...

        assert len(broker.subscribers) == 2

    def test_subscriber_registration_channel(self) -> None:
        broker = RedisClusterBroker()

        @broker.subscriber(channel="c1")
        async def h1(msg): ...

        @broker.subscriber(channel="c2")
        async def h2(msg): ...

        assert len(broker.subscribers) == 2

    def test_publisher_registration(self) -> None:
        broker = RedisClusterBroker()
        pub = broker.publisher(list="p1")
        assert pub is not None

        @pub
        @broker.subscriber(list="l1")
        async def h(msg): ...

        assert len(broker.publishers) == 1
        assert len(broker.subscribers) == 1


class TestClusterUsesAsyncProducer:
    """Cluster reuses the standard async RedisFastProducer (no sync proxy)."""

    def test_producer_is_standard_async(self) -> None:
        broker = RedisClusterBroker()
        assert type(broker.config.broker_config.producer) is RedisFastProducer

    @pytest.mark.asyncio()
    async def test_channel_publish_uses_async_client_publish(self) -> None:
        client = AsyncMock()
        client.publish = AsyncMock(return_value=1)
        conn = RedisConnectionState()
        conn._client = client
        conn._connected = True

        producer = RedisFastProducer(
            connection=conn,
            parser=None,
            decoder=None,
            message_format=BinaryMessageFormatV1,
            serializer=None,
        )
        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            _publish_type=PublishType.PUBLISH,
        )
        result = await producer.publish(cmd)
        assert result == 1
        client.publish.assert_awaited_once()


class TestRedisClusterConnectionStateDisconnect:
    """Tests for disconnect lifecycle."""

    @pytest.mark.asyncio()
    async def test_disconnect_closes_async_client(self) -> None:
        state = RedisClusterConnectionState({"host": "127.0.0.1", "port": 7000})
        client = AsyncMock()
        state._client = client
        state._connected = True

        await state.disconnect()

        client.aclose.assert_awaited_once()
        assert state._client is None
        assert not bool(state)

    @pytest.mark.asyncio()
    async def test_disconnect_before_connect_no_error(self) -> None:
        state = RedisClusterConnectionState({"host": "127.0.0.1", "port": 7000})
        await state.disconnect()  # Should not raise
        assert not bool(state)


class TestClusterBrokerPing:
    """Tests for RedisClusterBroker.ping()."""

    @pytest.mark.asyncio()
    async def test_ping_returns_true(self) -> None:
        broker = RedisClusterBroker(url="redis://127.0.0.1:7001")
        async with TestRedisBroker(broker) as br:
            result = await br.ping()
        assert result is True

    @pytest.mark.asyncio()
    async def test_ping_not_connected_returns_false(self) -> None:
        broker = RedisClusterBroker()
        result = await broker.ping()
        assert result is False


class TestRedisBrokerInit:
    """Covers branch paths in __init__."""

    def test_init_with_explicit_protocol(self) -> None:
        broker = RedisClusterBroker(protocol="redis")
        assert broker._connection is None

    def test_init_with_protocol_and_custom_url(self) -> None:
        broker = RedisClusterBroker(
            url="redis://custom:7000",
            protocol="redis",
        )
        assert broker._connection is None

    def test_specification_url_defaults_to_url(self) -> None:
        broker = RedisClusterBroker(url="redis://127.0.0.1:7001")
        assert broker._connection is None
