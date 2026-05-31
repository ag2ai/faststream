import pytest
from redis.asyncio.sentinel import SentinelConnectionPool

from faststream.exceptions import SetupError
from faststream.redis import RedisBroker
from faststream.redis.configs.state import RedisConnectionState, SentinelConfig

SENTINELS = [("sentinel-1", 26379), ("sentinel-2", 26379)]


@pytest.mark.redis()
class TestSentinelConfigUnit:
    """Unit tests for Sentinel mode (no running Redis/Sentinel needed)."""

    def test_sentinel_mode_builds_config(self) -> None:
        broker = RedisBroker(sentinels=SENTINELS, sentinel_master_name="mymaster")
        connection = broker.config.broker_config.connection
        assert isinstance(connection._sentinel, SentinelConfig)
        assert connection._sentinel.master_name == "mymaster"
        assert list(connection._sentinel.sentinels) == SENTINELS

    def test_sentinel_kwargs_stored(self) -> None:
        broker = RedisBroker(
            sentinels=SENTINELS,
            sentinel_master_name="mymaster",
            sentinel_kwargs={"socket_timeout": 1.0},
        )
        sentinel = broker.config.broker_config.connection._sentinel
        assert sentinel is not None
        assert sentinel.sentinel_kwargs == {"socket_timeout": 1.0}

    def test_direct_mode_has_no_sentinel(self) -> None:
        broker = RedisBroker("redis://localhost:6379")
        assert broker.config.broker_config.connection._sentinel is None

    def test_sentinel_requires_master_name(self) -> None:
        with pytest.raises(SetupError, match="sentinel_master_name"):
            RedisBroker(sentinels=SENTINELS)

    @pytest.mark.asyncio()
    async def test_connect_builds_sentinel_pool(self) -> None:
        state = RedisConnectionState(
            {"host": "localhost", "port": 6379, "db": 1},
            sentinel=SentinelConfig(sentinels=SENTINELS, master_name="mymaster"),
        )
        client = await state.connect()
        try:
            assert isinstance(client.connection_pool, SentinelConnectionPool)
            assert client.connection_pool.service_name == "mymaster"
        finally:
            await state.disconnect()
