"""Unit tests for RedisClusterBroker and ClusterConnectionState."""

import pytest

from faststream.redis import RedisClusterBroker, TestRedisClusterBroker
from faststream.redis.configs.state import (
    _CLUSTER_UNSUPPORTED_KEYS,
    ClusterConnectionState,
)

# --- ClusterConnectionState ---


@pytest.mark.redis()
def test_cluster_state_filters_unsupported_keys() -> None:
    options = {
        "host": "localhost",
        "port": 6379,
        "db": 1,
        "socket_read_size": 65536,
        "socket_type": 0,
        "retry_on_timeout": True,
        "parser_class": object,
        "encoder_class": object,
        "connection_class": object,
        "decode_responses": True,
        "encoding": "utf-8",
    }
    state = ClusterConnectionState(options)
    for key in _CLUSTER_UNSUPPORTED_KEYS:
        assert key not in state._options
    # Valid keys should be preserved
    assert state._options["host"] == "localhost"
    assert state._options["port"] == 6379
    assert state._options["encoding"] == "utf-8"


@pytest.mark.redis()
def test_cluster_state_defaults_max_connections() -> None:
    state = ClusterConnectionState({})
    assert state._options["max_connections"] == 2**31


@pytest.mark.redis()
def test_cluster_state_preserves_max_connections() -> None:
    state = ClusterConnectionState({"max_connections": 100})
    assert state._options["max_connections"] == 100


@pytest.mark.redis()
def test_cluster_state_not_connected_initially() -> None:
    state = ClusterConnectionState()
    assert not state


@pytest.mark.redis()
def test_cluster_state_client_raises_when_not_connected() -> None:
    state = ClusterConnectionState()
    with pytest.raises(Exception, match="Connection is not available"):
        _ = state.client


# --- RedisClusterBroker ---


@pytest.mark.redis()
def test_cluster_broker_instantiates() -> None:
    broker = RedisClusterBroker("redis://localhost:6379")
    assert broker is not None


@pytest.mark.redis()
def test_cluster_broker_uses_cluster_connection_state() -> None:
    broker = RedisClusterBroker("redis://localhost:7000")
    connection = broker.config.broker_config.connection
    assert isinstance(connection, ClusterConnectionState)


@pytest.mark.redis()
def test_cluster_broker_filters_db_from_url() -> None:
    """Even if the URL has /1, the cluster state should strip db."""
    broker = RedisClusterBroker("redis://localhost:7000/1")
    connection = broker.config.broker_config.connection
    assert "db" not in connection._options


@pytest.mark.redis()
def test_cluster_broker_subscriber_registration() -> None:
    """Subscribers can be registered on the cluster broker."""
    broker = RedisClusterBroker()

    @broker.subscriber(stream="test-stream")
    async def handler(msg: str) -> None:
        pass

    assert len(list(broker.subscribers)) == 1


@pytest.mark.redis()
def test_cluster_broker_publisher_registration() -> None:
    """Publishers can be registered on the cluster broker."""
    broker = RedisClusterBroker()
    pub = broker.publisher(stream="test-stream")
    assert pub is not None


# --- TestRedisClusterBroker ---


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_test_cluster_broker_publishes() -> None:
    broker = RedisClusterBroker()
    received = []

    @broker.subscriber(list="test-list")
    async def handler(msg: str) -> None:
        received.append(msg)

    async with TestRedisClusterBroker(broker) as br:
        await br.publish("hello", list="test-list")

    assert received == ["hello"]


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_test_cluster_broker_stream() -> None:
    broker = RedisClusterBroker()
    received = []

    @broker.subscriber(stream="test-stream")
    async def handler(msg: str) -> None:
        received.append(msg)

    async with TestRedisClusterBroker(broker) as br:
        await br.publish("world", stream="test-stream")

    assert received == ["world"]


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_test_cluster_broker_channel() -> None:
    broker = RedisClusterBroker()
    received = []

    @broker.subscriber(channel="test-channel")
    async def handler(msg: str) -> None:
        received.append(msg)

    async with TestRedisClusterBroker(broker) as br:
        await br.publish("hi", channel="test-channel")

    assert received == ["hi"]
