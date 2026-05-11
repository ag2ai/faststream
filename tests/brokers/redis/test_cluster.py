import pytest

from faststream.redis import RedisClusterBroker, RedisRouter

pytestmark = pytest.mark.redis_cluster


class TestClusterBrokerConfig:
    """Unit tests for RedisClusterBroker instantiation and configuration."""

    def test_default_instantiation(self) -> None:
        broker = RedisClusterBroker()
        assert broker._connection is None
        s = broker.config.broker_config.connection
        nodes = s._options.get("startup_nodes", [])
        assert len(nodes) == 1
        assert nodes[0].host == "127.0.0.1"

    def test_url_parsing(self) -> None:
        broker = RedisClusterBroker(url="redis://cluster.example.com:6380")
        s = broker.config.broker_config.connection
        nodes = s._options.get("startup_nodes", [])
        assert len(nodes) == 1
        assert nodes[0].host == "cluster.example.com"
        assert nodes[0].port == 6380

    def test_explicit_host_port(self) -> None:
        broker = RedisClusterBroker(host="myhost", port=7000)
        s = broker.config.broker_config.connection
        nodes = s._options.get("startup_nodes", [])
        assert len(nodes) == 1
        assert nodes[0].host == "myhost"
        assert nodes[0].port == 7000

    def test_startup_nodes(self) -> None:
        broker = RedisClusterBroker(
            url="redis://node1:7000",
            startup_nodes=[("node2", 7001), ("node3", 7002)],
        )
        s = broker.config.broker_config.connection
        nodes = s._options.get("startup_nodes", [])
        assert len(nodes) == 3
        assert nodes[0].host == "node1"
        assert nodes[0].port == 7000
        assert nodes[1].host == "node2"
        assert nodes[1].port == 7001
        assert nodes[2].host == "node3"
        assert nodes[2].port == 7002

    def test_db_filtered_out(self) -> None:
        """Db should be removed from cluster connection options."""
        broker = RedisClusterBroker(url="redis://localhost:6379/5")
        s = broker.config.broker_config.connection
        assert "db" not in s._options

    def test_incompatible_params_filtered(self) -> None:
        """Cluster-incompatible params should be stripped from options."""
        broker = RedisClusterBroker()
        s = broker.config.broker_config.connection
        for key in (
            "socket_read_size",
            "socket_type",
            "retry_on_timeout",
            "parser_class",
            "encoder_class",
        ):
            assert key not in s._options, f"{key} should NOT be in options"

    def test_horizontal_override_url(self) -> None:
        """Explicit host/port should override URL values."""
        broker = RedisClusterBroker(
            url="redis://wrong:6380",
            host="correct",
            port=7000,
        )
        s = broker.config.broker_config.connection
        nodes = s._options.get("startup_nodes", [])
        assert len(nodes) == 1
        assert nodes[0].host == "correct"
        assert nodes[0].port == 7000

    def test_specification_url(self) -> None:
        broker = RedisClusterBroker(specification_url="redis://spec-host:7000")
        assert broker.specification.url == ["redis://spec-host:7000"]

    def test_protocol_detection(self) -> None:
        broker = RedisClusterBroker()
        assert broker.specification.protocol == "redis"

    def test_protocol_detection_rediss(self) -> None:
        broker = RedisClusterBroker(url="rediss://localhost:7000")
        assert broker.specification.protocol == "rediss"

    def test_channel_subscriber(self) -> None:
        """Channel subscriber works."""
        broker = RedisClusterBroker()

        @broker.subscriber("test-channel")
        async def handler(msg: str) -> None: ...

        assert len(broker.subscribers) == 1

    def test_list_subscriber_works(self) -> None:
        broker = RedisClusterBroker()

        @broker.subscriber(list="test-list")
        async def handler(msg: str) -> None: ...

        assert len(broker.subscribers) == 1

    def test_stream_subscriber_works(self) -> None:
        broker = RedisClusterBroker()

        @broker.subscriber(stream="test-stream")
        async def handler(msg: str) -> None: ...

        assert len(broker.subscribers) == 1

    def test_inherited_publisher_registration(self) -> None:
        broker = RedisClusterBroker()

        pub = broker.publisher(list="pub-list")
        assert pub is not None

        @broker.subscriber(list="test-list")
        @pub
        async def handler(msg: str) -> None: ...

        assert len(broker.publishers) == 1
        assert len(broker.subscribers) == 1

    def test_router_inclusion(self) -> None:
        broker = RedisClusterBroker(routers=[RedisRouter()])
        assert len(broker.routers) == 1

    def test_connection_state_type(self) -> None:
        from faststream.redis.configs.state import RedisClusterConnectionState

        broker = RedisClusterBroker()
        assert isinstance(
            broker.config.broker_config.connection,
            RedisClusterConnectionState,
        )

    def test_message_format_default(self) -> None:
        from faststream.redis.parser import BinaryMessageFormatV1

        broker = RedisClusterBroker()
        assert broker.message_format is BinaryMessageFormatV1

    def test_full_redis_broker_compatible_api(self) -> None:
        """All RedisBroker __init__ params should be accepted (drop-in compatible)."""
        from redis.asyncio.connection import DefaultParser, Encoder

        broker = RedisClusterBroker(
            url="redis://localhost:6379",
            host="localhost",
            port=6379,
            db=0,
            client_name="test",
            health_check_interval=10,
            max_connections=10,
            socket_timeout=5.0,
            socket_connect_timeout=3.0,
            socket_read_size=65536,
            socket_keepalive=True,
            retry_on_timeout=True,
            encoding="utf-8",
            encoding_errors="strict",
            parser_class=DefaultParser,
            encoder_class=Encoder,
            graceful_timeout=30.0,
        )
        assert broker is not None
        assert len(broker.config.broker_config.connection._options["startup_nodes"]) == 1


class TestRedisClusterConnectionState:
    """Unit tests for RedisClusterConnectionState."""

    def test_initial_state(self) -> None:
        from faststream.redis.configs.state import RedisClusterConnectionState

        state = RedisClusterConnectionState()
        assert not state
        assert state._client is None

    def test_initial_state_with_options(self) -> None:
        from faststream.redis.configs.state import RedisClusterConnectionState

        state = RedisClusterConnectionState({"host": "127.0.0.1", "port": 7000})
        assert not state

    def test_client_raises_before_connect(self) -> None:
        from faststream.exceptions import IncorrectState
        from faststream.redis.configs.state import RedisClusterConnectionState

        state = RedisClusterConnectionState()
        with pytest.raises(IncorrectState):
            _ = state.client

    def test_bool_false_before_connect(self) -> None:
        from faststream.redis.configs.state import RedisClusterConnectionState

        state = RedisClusterConnectionState()
        assert not bool(state)

    def test_options_stored(self) -> None:
        from faststream.redis.configs.state import RedisClusterConnectionState

        state = RedisClusterConnectionState({"startup_nodes": ["node1"]})
        assert state._options["startup_nodes"] == ["node1"]


class TestClusterBrokerInheritance:
    """Ensure RedisClusterBroker properly inherits from RedisBroker."""

    def test_is_redis_broker_subclass(self) -> None:
        from faststream.redis import RedisBroker

        assert issubclass(RedisClusterBroker, RedisBroker)

    def test_has_required_methods(self) -> None:
        broker = RedisClusterBroker()
        for attr in (
            "start",
            "stop",
            "ping",
            "publish",
            "publish_batch",
            "request",
            "subscriber",
            "publisher",
            "include_router",
        ):
            assert hasattr(broker, attr)

    def test_broker_uses_config_composition(self) -> None:
        from faststream._internal.configs import ConfigComposition

        broker = RedisClusterBroker()
        assert isinstance(broker.config, ConfigComposition)

    def test_start_stop_lifecycle(self) -> None:
        import anyio

        async def test() -> None:
            broker = RedisClusterBroker()
            assert broker._connection is None
            await broker.start()
            assert broker._connection is not None
            await broker.stop()
            assert broker._connection is None

        anyio.run(test)


@pytest.mark.parametrize(
    ("url", "expected_host", "expected_port"),
    (
        ("redis://a:7000", "a", 7000),
        ("redis://b:7001", "b", 7001),
        ("redis://127.0.0.1:7000", "127.0.0.1", 7000),
    ),
)
def test_parametrized_urls(url, expected_host, expected_port) -> None:
    broker = RedisClusterBroker(url=url)
    s = broker.config.broker_config.connection
    nodes = s._options.get("startup_nodes", [])
    assert len(nodes) == 1
    assert nodes[0].host == expected_host
    assert nodes[0].port == expected_port


@pytest.mark.parametrize(
    "kwargs",
    (
        {"host": "h1", "port": "7000"},
        {"host": "h1", "port": 7000},
        {"url": "redis://h1:7000"},
    ),
)
def test_different_parameter_forms(kwargs) -> None:
    broker = RedisClusterBroker(**kwargs)
    s = broker.config.broker_config.connection
    assert len(s._options["startup_nodes"]) == 1


def test_exported_from_redis_package() -> None:
    from faststream.redis import RedisClusterBroker as Exported

    assert Exported is RedisClusterBroker


def test_exported_from_broker_subpackage() -> None:
    from faststream.redis.broker import RedisClusterBroker as Exported

    assert Exported is RedisClusterBroker
