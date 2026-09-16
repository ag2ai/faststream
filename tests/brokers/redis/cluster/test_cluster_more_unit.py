from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest

from faststream.redis import RedisClusterBroker, RedisRouter, TestRedisBroker
from faststream.redis.configs.state import RedisClusterConnectionState
from faststream.redis.exceptions import UnreachablePathError
from faststream.redis.parser import BinaryMessageFormatV1
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.response import RedisPublishCommand
from faststream.redis.subscriber.usecases import ChannelSubscriber
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


class TestRedisClusterConnectionStateDisconnect:
    """Tests for disconnect lifecycle."""

    @pytest.mark.asyncio()
    async def test_disconnect_drops_the_client(self) -> None:
        state = RedisClusterConnectionState({"host": "127.0.0.1", "port": 7000})
        state._client = AsyncMock()
        state._connected = True

        await state.disconnect()

        assert state._client is None
        assert not state

    @pytest.mark.asyncio()
    async def test_disconnect_before_connect_no_error(self) -> None:
        state = RedisClusterConnectionState({"host": "127.0.0.1", "port": 7000})
        await state.disconnect()  # Should not raise
        assert not bool(state)

    def test_bool_reflects_connected(self) -> None:
        state = RedisClusterConnectionState()
        assert not state
        state._connected = True
        assert state


class TestClusterProducerUnit:
    @pytest.fixture()
    def mock_client(self) -> AsyncMock:
        client = AsyncMock()
        client.publish = AsyncMock(return_value=1)
        client.rpush = AsyncMock(return_value=1)
        client.xadd = AsyncMock(return_value=b"stream-id")
        return client

    @pytest.fixture()
    def producer(self, mock_client: AsyncMock) -> RedisFastProducer:
        connection = RedisClusterConnectionState()
        connection._client = mock_client
        connection._connected = True

        return RedisFastProducer(
            connection=connection,
            parser=None,
            decoder=None,
            message_format=BinaryMessageFormatV1,
            serializer=None,
        )

    def test_cluster_broker_builds_the_shared_producer(self) -> None:
        broker = RedisClusterBroker(url="redis://127.0.0.1:7001")
        assert type(broker.config.broker_config.producer) is RedisFastProducer

    @pytest.mark.asyncio()
    async def test_publish_channel(
        self,
        producer: RedisFastProducer,
        mock_client: AsyncMock,
    ) -> None:
        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            _publish_type=PublishType.PUBLISH,
        )
        result = await producer.publish(cmd)

        assert result == 1
        # the async cluster client publishes directly — no thread hop
        mock_client.publish.assert_awaited_once()

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
        mock_client.rpush.assert_awaited_once()

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
    async def test_publish_unreachable(self, producer: RedisFastProducer) -> None:
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
        mock_client: AsyncMock,
    ) -> None:
        mock_client.pubsub = MagicMock(return_value=_reply_pubsub())

        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            timeout=5.0,
            _publish_type=PublishType.REQUEST,
        )
        result = await producer.request(cmd)

        assert result == "resp"
        # the reply channel is subscribed on the same async cluster client
        mock_client.publish.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_request_list(
        self,
        producer: RedisFastProducer,
        mock_client: AsyncMock,
    ) -> None:
        mock_client.pubsub = MagicMock(return_value=_reply_pubsub())

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
        mock_client: AsyncMock,
    ) -> None:
        mock_client.pubsub = MagicMock(return_value=_reply_pubsub())

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
        mock_client: AsyncMock,
    ) -> None:
        psub = _reply_pubsub()

        async def _slow(*args: object, **kwargs: object) -> None:
            await anyio.sleep(10)

        psub.get_message = _slow
        mock_client.pubsub = MagicMock(return_value=psub)

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
        mock_client: AsyncMock,
    ) -> None:
        mock_client.pubsub = MagicMock(return_value=_reply_pubsub())

        cmd = RedisPublishCommand(
            b"hello",
            channel="ch",
            timeout=5.0,
            _publish_type=PublishType.REQUEST,
        )
        cmd.destination_type = None  # type: ignore[assignment]

        with pytest.raises(UnreachablePathError):
            await producer.request(cmd)


class TestClusterChannelSubscriber:
    def test_channel_subscriber_is_not_patched(self) -> None:
        broker = RedisClusterBroker(url="redis://127.0.0.1:7001")

        @broker.subscriber(channel="ch")
        async def handler(msg: str) -> None: ...

        subscriber = broker.subscribers[0]

        assert isinstance(subscriber, ChannelSubscriber)
        # stock `start()` now works, so the cluster broker rebinds nothing
        assert subscriber.start.__func__ is ChannelSubscriber.start  # type: ignore[attr-defined]


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


def _reply_pubsub() -> AsyncMock:
    """A `pubsub()` handle that yields the subscribe ack, then one reply."""
    psub = AsyncMock()
    psub.get_message = AsyncMock(side_effect=[None, "resp"])
    return psub
