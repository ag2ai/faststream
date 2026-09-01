from importlib.metadata import version
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest
from redis.credentials import CredentialProvider

from faststream.exceptions import IncorrectState
from faststream.redis import RedisClusterBroker, RedisRouter, TestRedisBroker
from faststream.redis._compat import REDIS_V720
from faststream.redis.configs import (
    ConnectionState,
    RedisConnectionState,
    state as state_module,
)
from faststream.redis.configs.state import RedisClusterConnectionState
from faststream.redis.parser import BinaryMessageFormatV1
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.response import RedisPublishCommand
from faststream.response.publish_type import PublishType


def test_redis_v720_uses_lexicographic_version_comparison() -> None:
    major, minor, *_ = version("redis").split(".")
    assert REDIS_V720 is ((int(major), int(minor)) >= (7, 2))


def test_driver_info_avoids_deprecated_kwargs() -> None:
    kwargs = state_module._get_driver_info()
    if REDIS_V720:
        assert set(kwargs) == {"driver_info"}
    else:
        assert set(kwargs) == {"lib_name", "lib_version"}


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


class TestRedisClusterConnectionStateConnect:
    @pytest.mark.asyncio()
    @pytest.mark.parametrize("available_method", ("publish", "pubsub"))
    async def test_native_pubsub_guard_runs_before_client_construction(
        self,
        monkeypatch: pytest.MonkeyPatch,
        available_method: str,
    ) -> None:
        constructed = False

        class IncompatibleCluster:
            def __init__(self, **kwargs: object) -> None:
                nonlocal constructed
                constructed = True

        setattr(IncompatibleCluster, available_method, lambda self: None)
        monkeypatch.setattr(state_module, "RedisCluster", IncompatibleCluster)
        get_driver_info = MagicMock()
        monkeypatch.setattr(state_module, "_get_driver_info", get_driver_info)

        state = RedisClusterConnectionState()
        with pytest.raises(IncorrectState, match=r"redis-py >= 8\.0\.0"):
            await state.connect()

        assert not constructed
        get_driver_info.assert_not_called()
        assert state._client is None
        assert not state

    @pytest.mark.asyncio()
    async def test_connect_passes_native_client_options(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured: dict[str, object] = {}
        initialized = False

        class NativeCluster:
            def __init__(self, **kwargs: object) -> None:
                captured.update(kwargs)

            async def publish(self) -> None: ...

            def pubsub(self) -> None: ...

            async def initialize(self) -> None:
                nonlocal initialized
                initialized = True

        driver_info = object()
        credential_provider = object()
        monkeypatch.setattr(state_module, "RedisCluster", NativeCluster)
        monkeypatch.setattr(
            state_module,
            "_get_driver_info",
            lambda: {"driver_info": driver_info},
        )

        state = RedisClusterConnectionState({
            "host": "127.0.0.1",
            "port": 7000,
            "credential_provider": credential_provider,
            "socket_timeout": None,
        })
        client = await state.connect()

        assert client is state.client
        assert captured == {
            "host": "127.0.0.1",
            "port": 7000,
            "credential_provider": credential_provider,
            "legacy_responses": True,
            "driver_info": driver_info,
        }
        assert "lib_name" not in captured
        assert "lib_version" not in captured
        assert initialized
        assert state

    @pytest.mark.asyncio()
    async def test_connect_respects_explicit_legacy_responses(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured: dict[str, object] = {}

        class NativeCluster:
            def __init__(self, **kwargs: object) -> None:
                captured.update(kwargs)

            async def publish(self) -> None: ...

            def pubsub(self) -> None: ...

            async def initialize(self) -> None: ...

        monkeypatch.setattr(state_module, "RedisCluster", NativeCluster)
        monkeypatch.setattr(state_module, "_get_driver_info", dict)

        state = RedisClusterConnectionState({"legacy_responses": False})
        await state.connect()

        assert captured["legacy_responses"] is False

    @pytest.mark.asyncio()
    async def test_connect_closes_client_when_initialize_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        closed = False

        class FailingCluster:
            def __init__(self, **kwargs: object) -> None: ...

            async def publish(self) -> None: ...

            def pubsub(self) -> None: ...

            async def initialize(self) -> None:
                msg = "cluster discovery failed"
                raise RuntimeError(msg)

            async def aclose(self) -> None:
                nonlocal closed
                closed = True

        monkeypatch.setattr(state_module, "RedisCluster", FailingCluster)
        monkeypatch.setattr(state_module, "_get_driver_info", dict)

        state = RedisClusterConnectionState()
        with pytest.raises(RuntimeError, match="cluster discovery failed"):
            await state.connect()

        assert closed
        assert state._client is None
        assert not state

    @pytest.mark.asyncio()
    async def test_connect_returns_existing_client_without_reconstruction(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client = MagicMock()
        state = RedisClusterConnectionState()
        state._client = client
        state._connected = True
        constructor = MagicMock()
        monkeypatch.setattr(state_module, "RedisCluster", constructor)

        assert await state.connect() is client
        constructor.assert_not_called()


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

    def test_credential_provider_reaches_cluster_connection(self) -> None:
        credential_provider = MagicMock(spec=CredentialProvider)
        broker = RedisClusterBroker(
            credential_provider=credential_provider,
        )

        assert (
            broker.config.broker_config.connection._options["credential_provider"]
            is credential_provider
        )

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
        assert not state

    @pytest.mark.asyncio()
    async def test_disconnect_before_connect_no_error(self) -> None:
        state = RedisClusterConnectionState({"host": "127.0.0.1", "port": 7000})
        await state.disconnect()  # Should not raise
        assert not bool(state)


class TestClusterNativeProducerUnit:
    """The cluster broker uses the shared native async producer path."""

    def test_broker_uses_standard_producer(self) -> None:
        broker = RedisClusterBroker()
        assert type(broker.config.broker_config.producer) is RedisFastProducer

    @pytest.fixture()
    def mock_client(self) -> MagicMock:
        client = MagicMock()
        client.publish = AsyncMock(return_value=1)
        client.rpush = AsyncMock(return_value=1)
        client.xadd = AsyncMock(return_value=b"stream-id")
        psub = MagicMock()
        psub.subscribe = AsyncMock()
        psub.get_message = AsyncMock(side_effect=[None, "resp"])
        psub.unsubscribe = AsyncMock()
        psub.aclose = AsyncMock()
        client.pubsub.return_value = psub
        return client

    @pytest.fixture()
    def mock_connection(self, mock_client: MagicMock) -> ConnectionState[Any]:
        conn = RedisConnectionState()
        conn._client = mock_client
        conn._connected = True
        return conn

    @pytest.fixture()
    def producer(
        self,
        mock_connection: ConnectionState,
    ) -> RedisFastProducer:
        return RedisFastProducer(
            connection=mock_connection,
            parser=None,
            decoder=None,
            message_format=BinaryMessageFormatV1,
            serializer=None,
        )

    @pytest.mark.asyncio()
    async def test_publish_channel(
        self,
        producer: RedisFastProducer,
        mock_client: MagicMock,
    ) -> None:
        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            _publish_type=PublishType.PUBLISH,
        )
        result = await producer.publish(cmd)
        assert result == 1
        mock_client.publish.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_publish_list(
        self,
        producer: RedisFastProducer,
        mock_client: MagicMock,
    ) -> None:
        cmd = RedisPublishCommand(
            b"hello",
            list="lst",
            _publish_type=PublishType.PUBLISH,
        )
        result = await producer.publish(cmd)
        assert result == 1
        mock_client.rpush.assert_awaited_once_with(
            "lst", mock_client.rpush.call_args[0][1]
        )

    @pytest.mark.asyncio()
    async def test_publish_stream(
        self,
        producer: RedisFastProducer,
        mock_client: MagicMock,
    ) -> None:
        cmd = RedisPublishCommand(
            b"hello",
            stream="strm",
            maxlen=100,
            _publish_type=PublishType.PUBLISH,
        )
        result = await producer.publish(cmd)
        assert result == b"stream-id"
        mock_client.xadd.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_publish_unreachable(
        self,
        producer: RedisFastProducer,
    ) -> None:
        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            _publish_type=PublishType.PUBLISH,
        )
        cmd.destination_type = None  # type: ignore[assignment]
        with pytest.raises(AssertionError, match="unreachable"):
            await producer.publish(cmd)

    @pytest.mark.asyncio()
    async def test_request_channel(
        self,
        producer: RedisFastProducer,
        mock_client: MagicMock,
    ) -> None:
        psub = mock_client.pubsub.return_value

        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            timeout=5.0,
            _publish_type=PublishType.REQUEST,
        )
        result = await producer.request(cmd)
        assert result == "resp"
        mock_client.publish.assert_awaited_once()
        psub.unsubscribe.assert_awaited_once()
        psub.aclose.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_request_list(
        self,
        producer: RedisFastProducer,
        mock_client: MagicMock,
    ) -> None:
        psub = mock_client.pubsub.return_value

        cmd = RedisPublishCommand(
            b"hello",
            list="lst",
            timeout=5.0,
            _publish_type=PublishType.REQUEST,
        )
        result = await producer.request(cmd)
        assert result == "resp"
        mock_client.rpush.assert_awaited_once()
        psub.unsubscribe.assert_awaited_once()
        psub.aclose.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_request_stream(
        self,
        producer: RedisFastProducer,
        mock_client: MagicMock,
    ) -> None:
        psub = mock_client.pubsub.return_value

        cmd = RedisPublishCommand(
            b"hello",
            stream="strm",
            maxlen=100,
            timeout=5.0,
            _publish_type=PublishType.REQUEST,
        )
        result = await producer.request(cmd)
        assert result == "resp"
        mock_client.xadd.assert_awaited_once()
        psub.unsubscribe.assert_awaited_once()
        psub.aclose.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_request_timeout(
        self,
        producer: RedisFastProducer,
        mock_client: MagicMock,
    ) -> None:
        """Timeout inside fail_after raises TimeoutError."""
        psub = mock_client.pubsub.return_value

        async def _slow(*args: object, **kwargs: object) -> None:
            await anyio.sleep(10)

        psub.get_message = _slow

        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            timeout=0.05,
            _publish_type=PublishType.REQUEST,
        )
        with pytest.raises(TimeoutError):
            await producer.request(cmd)
        psub.unsubscribe.assert_awaited_once()
        psub.aclose.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_request_unreachable(
        self,
        producer: RedisFastProducer,
        mock_client: MagicMock,
    ) -> None:
        psub = mock_client.pubsub.return_value

        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            timeout=5.0,
            _publish_type=PublishType.REQUEST,
        )
        cmd.destination_type = None  # type: ignore[assignment]
        with pytest.raises(AssertionError, match="unreachable"):
            await producer.request(cmd)
        psub.unsubscribe.assert_awaited_once()
        psub.aclose.assert_awaited_once()


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
        # specification_url was set from url in __init__
        assert broker._connection is None
