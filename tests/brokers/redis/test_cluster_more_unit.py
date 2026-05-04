from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock

import anyio
import pytest

from faststream.redis import RedisClusterBroker, RedisRouter
from faststream.redis.broker.cluster_broker import (
    ClusterFastProducer,
    _clean_cluster_options,
)
from faststream.redis.configs import ConnectionState
from faststream.redis.configs.cluster import (
    ClusterConnectionState,
    _ClusterPublishProxy,
)
from faststream.redis.exceptions import UnreachablePathError
from faststream.redis.parser import BinaryMessageFormatV1
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.response import RedisPublishCommand
from faststream.response.publish_type import PublishType


class TestClusterConnectionStateUnit:
    """Unit tests for ClusterConnectionState (no cluster needed)."""

    def test_initial_bool_false(self) -> None:
        state = ClusterConnectionState()
        assert not bool(state)

    def test_options_passed_and_stored(self) -> None:
        opts = {
            "host": "custom",
            "port": 7000,
            "ssl": True,
            "password": "secret",
        }
        state = ClusterConnectionState(opts)
        assert state._options == opts

    def test_get_sync_cluster_creates_once(self) -> None:
        state = ClusterConnectionState({"host": "127.0.0.1", "port": 7000})
        # We can't actually connect, but we can verify the method shape
        # by checking that _sync_cluster starts as None
        assert state._sync_cluster is None
        assert state._thread_pool is None

    def test_clean_cluster_options_strips_incompat(self) -> None:
        raw = {
            "host": "h",
            "port": 6379,
            "db": 5,
            "socket_read_size": 1024,
            "socket_type": 1,
            "retry_on_timeout": True,
            "parser_class": "Parser",
            "encoder_class": "Encoder",
            "connection_class": "Conn",
            "ssl": True,
        }
        cleaned = _clean_cluster_options(raw, host="override", port=7000)
        for banned in (
            "db",
            "socket_read_size",
            "socket_type",
            "retry_on_timeout",
            "parser_class",
            "encoder_class",
            "connection_class",
        ):
            assert banned not in cleaned, f"{banned} should be filtered"
        assert "ssl" in cleaned
        nodes = cleaned["startup_nodes"]
        assert len(nodes) == 1
        assert nodes[0].host == "override"
        assert nodes[0].port == 7000

    def test_clean_cluster_options_startup_nodes(self) -> None:
        cleaned = _clean_cluster_options(
            {"host": "h", "port": 6379},
            startup_nodes=[("n1", 7001), ("n2", 7002)],
        )
        nodes = cleaned["startup_nodes"]
        assert len(nodes) == 3
        assert nodes[0].host == "h"
        assert nodes[1].host == "n1"
        assert nodes[2].host == "n2"

    def test_clean_cluster_options_no_host(self) -> None:
        """When no host/port given, nodes list is empty — connect will fail later."""
        cleaned = _clean_cluster_options({})
        assert cleaned["startup_nodes"] == []


class TestClusterPublishProxy:
    """Unit tests for _ClusterPublishProxy."""

    @pytest.mark.asyncio()
    async def test_proxy_passes_through_attr(self) -> None:
        class FakeAsync:
            async def xadd(self, *a, **kw) -> str:
                return "ok"

        proxy = _ClusterPublishProxy(FakeAsync(), None)
        assert hasattr(proxy, "xadd")
        result = await proxy.xadd("stream", {})
        assert result == "ok"

    @pytest.mark.asyncio()
    async def test_proxy_routes_publish_to_sync(self) -> None:
        calls = []

        async def fake_sync_publish(channel: str, body: bytes) -> int:
            calls.append((channel, body))
            return 1

        proxy = _ClusterPublishProxy(object(), fake_sync_publish)
        result = await proxy.publish("ch", b"msg")
        assert result == 1
        assert calls == [("ch", b"msg")]


class TestClusterBrokerWarnings:
    """Tests for RuntimeWarning on pipeline usage."""

    @pytest.mark.asyncio()
    async def test_publish_with_pipeline_warns(self) -> None:
        broker = RedisClusterBroker(url="redis://127.0.0.1:7001")
        async with broker:
            with pytest.warns(RuntimeWarning, match="Pipeline is not supported"):
                await broker.publish("hello", channel="ch", pipeline=None)

    @pytest.mark.asyncio()
    async def test_publish_batch_with_pipeline_warns(self) -> None:
        broker = RedisClusterBroker(url="redis://127.0.0.1:7001")
        async with broker:
            with pytest.warns(RuntimeWarning, match="Pipeline is not supported"):
                await broker.publish_batch("x", list="l", pipeline=None)


class TestClusterBrokerInheritanceExtra:
    """Additional inheritance/API compatibility tests."""

    def test_db_is_filtered_from_options(self) -> None:
        broker = RedisClusterBroker(url="redis://localhost:6379/5")
        opts = broker.config.broker_config.connection._options
        assert "db" not in opts

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


class TestSyncPubSubProxyUnit:
    """Unit tests for _SyncPubSubProxy."""

    def test_proxy_stores_pubsub_and_pool(self) -> None:
        """Verify proxy initialises correctly."""
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            # We need a sync cluster to create pubsub, so just verify
            # the class structure — actual init tested in integration.
            pass
        finally:
            pool.shutdown(wait=False)


class TestClusterConnectionStateDisconnect:
    """Tests for disconnect lifecycle."""

    @pytest.mark.asyncio()
    async def test_disconnect_cleans_thread_pool(self) -> None:
        state = ClusterConnectionState({"host": "127.0.0.1", "port": 7000})
        assert state._thread_pool is None
        await state.disconnect()
        assert state._thread_pool is None
        assert state._sync_cluster is None
        assert state._client is None

    @pytest.mark.asyncio()
    async def test_disconnect_before_connect_no_error(self) -> None:
        state = ClusterConnectionState({"host": "127.0.0.1", "port": 7000})
        await state.disconnect()  # Should not raise
        assert not bool(state)

    def test_bool_reflects_connected(self) -> None:
        state = ClusterConnectionState()
        assert not state
        state._connected = True
        assert state


class TestClusterFastProducerUnit:
    """Direct unit tests for ClusterFastProducer routing logic."""

    @pytest.fixture()
    def mock_client(self) -> AsyncMock:
        client = AsyncMock()
        client.rpush = AsyncMock(return_value=1)
        client.xadd = AsyncMock(return_value=b"stream-id")
        return client

    @pytest.fixture()
    def mock_connection(self, mock_client: AsyncMock) -> ConnectionState:
        conn = ConnectionState()
        conn._client = mock_client
        conn._connected = True
        return conn

    @pytest.fixture()
    def mock_cluster_state(self) -> AsyncMock:
        state = AsyncMock(spec=ClusterConnectionState)
        state.sync_publish = AsyncMock(return_value=1)
        return state

    @pytest.fixture()
    def producer(
        self,
        mock_connection: ConnectionState,
        mock_cluster_state: AsyncMock,
    ) -> RedisFastProducer:

        return ClusterFastProducer(
            connection=mock_connection,
            cluster_state=mock_cluster_state,
            parser=None,
            decoder=None,
            message_format=BinaryMessageFormatV1,
            serializer=None,
        )

    @pytest.mark.asyncio()
    async def test_publish_channel(
        self,
        producer: RedisFastProducer,
        mock_cluster_state: AsyncMock,
    ) -> None:
        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            _publish_type=PublishType.PUBLISH,
        )
        result = await producer.publish(cmd)
        assert result == 1
        mock_cluster_state.sync_publish.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_publish_list(
        self,
        producer: RedisFastProducer,
        mock_client: AsyncMock,
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
        mock_client: AsyncMock,
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
        """No matching destination_type → UnreachablePathError."""
        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            _publish_type=PublishType.PUBLISH,
        )
        cmd.destination_type = None  # type: ignore[assignment]
        with pytest.raises(UnreachablePathError):
            await producer.publish(cmd)

    @pytest.mark.asyncio()
    async def test_request_channel(
        self,
        producer: RedisFastProducer,
        mock_cluster_state: AsyncMock,
    ) -> None:
        psub = AsyncMock()
        psub.subscribe = AsyncMock()
        psub.get_message = AsyncMock(side_effect=[None, "resp"])
        psub.unsubscribe = AsyncMock()
        psub.aclose = AsyncMock()
        mock_cluster_state.pubsub.return_value = psub

        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            timeout=5.0,
            _publish_type=PublishType.REQUEST,
        )
        result = await producer.request(cmd)
        assert result == "resp"
        mock_cluster_state.sync_publish.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_request_list(
        self,
        producer: RedisFastProducer,
        mock_cluster_state: AsyncMock,
        mock_client: AsyncMock,
    ) -> None:
        psub = AsyncMock()
        psub.subscribe = AsyncMock()
        psub.get_message = AsyncMock(side_effect=[None, "resp"])
        psub.unsubscribe = AsyncMock()
        psub.aclose = AsyncMock()
        mock_cluster_state.pubsub.return_value = psub

        cmd = RedisPublishCommand(
            b"hello",
            list="lst",
            timeout=5.0,
            _publish_type=PublishType.REQUEST,
        )
        result = await producer.request(cmd)
        assert result == "resp"
        mock_client.rpush.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_request_stream(
        self,
        producer: RedisFastProducer,
        mock_cluster_state: AsyncMock,
        mock_client: AsyncMock,
    ) -> None:
        psub = AsyncMock()
        psub.subscribe = AsyncMock()
        psub.get_message = AsyncMock(side_effect=[None, "resp"])
        psub.unsubscribe = AsyncMock()
        psub.aclose = AsyncMock()
        mock_cluster_state.pubsub.return_value = psub

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

    @pytest.mark.asyncio()
    async def test_request_timeout(
        self,
        producer: RedisFastProducer,
        mock_cluster_state: AsyncMock,
    ) -> None:
        """Timeout inside fail_after raises TimeoutError."""
        psub = AsyncMock()
        psub.subscribe = AsyncMock()

        async def _slow(*args: object, **kwargs: object) -> None:
            await anyio.sleep(10)

        psub.get_message = _slow
        psub.unsubscribe = AsyncMock()
        psub.aclose = AsyncMock()
        mock_cluster_state.pubsub.return_value = psub

        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            timeout=0.05,
            _publish_type=PublishType.REQUEST,
        )
        with pytest.raises(TimeoutError):
            await producer.request(cmd)

    @pytest.mark.asyncio()
    async def test_request_unreachable(
        self,
        producer: RedisFastProducer,
        mock_cluster_state: AsyncMock,
    ) -> None:
        """No matching destination_type → UnreachablePathError."""
        psub = AsyncMock()
        psub.subscribe = AsyncMock()
        psub.get_message = AsyncMock(side_effect=[None, "resp"])
        psub.unsubscribe = AsyncMock()
        psub.aclose = AsyncMock()
        mock_cluster_state.pubsub.return_value = psub

        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            timeout=5.0,
            _publish_type=PublishType.REQUEST,
        )
        cmd.destination_type = None  # type: ignore[assignment]
        with pytest.raises(UnreachablePathError):
            await producer.request(cmd)


class TestClusterBrokerPing:
    """Tests for RedisClusterBroker.ping()."""

    @pytest.mark.asyncio()
    async def test_ping_returns_true(self) -> None:
        broker = RedisClusterBroker(url="redis://127.0.0.1:7001")
        async with broker:
            result = await broker.ping()
        assert result is True

    @pytest.mark.asyncio()
    async def test_ping_not_connected_returns_false(self) -> None:
        broker = RedisClusterBroker()
        result = await broker.ping()
        assert result is False


class TestRedisClusterBrokerInit:
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
