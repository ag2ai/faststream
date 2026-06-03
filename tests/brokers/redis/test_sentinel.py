import pytest
from redis.asyncio.sentinel import SentinelConnectionPool

from faststream.exceptions import SetupError
from faststream.redis import RedisSentinelBroker
from faststream.redis.configs.state import RedisSentinelConnectionState

SENTINELS = [("sentinel-1", 26379), ("sentinel-2", 26379)]


@pytest.mark.redis()
class TestRedisSentinelBrokerUnit:
    """Unit tests for RedisSentinelBroker (no running Redis/Sentinel needed)."""

    def test_builds_sentinel_connection_state(self) -> None:
        broker = RedisSentinelBroker(
            sentinels=SENTINELS, sentinel_master_name="mymaster"
        )
        connection = broker.config.broker_config.connection
        assert isinstance(connection, RedisSentinelConnectionState)
        assert connection._master_name == "mymaster"
        assert connection._sentinels == SENTINELS

    def test_sentinel_kwargs_stored(self) -> None:
        broker = RedisSentinelBroker(
            sentinels=SENTINELS,
            sentinel_master_name="mymaster",
            sentinel_kwargs={"socket_timeout": 1.0},
        )
        connection = broker.config.broker_config.connection
        assert connection._sentinel_kwargs == {"socket_timeout": 1.0}

    def test_requires_sentinels(self) -> None:
        with pytest.raises(SetupError, match="sentinels"):
            RedisSentinelBroker(sentinels=[], sentinel_master_name="mymaster")

    def test_requires_master_name(self) -> None:
        with pytest.raises(SetupError, match="sentinel_master_name"):
            RedisSentinelBroker(sentinels=SENTINELS)

    @pytest.mark.asyncio()
    async def test_connect_builds_sentinel_pool(self) -> None:
        broker = RedisSentinelBroker(
            sentinels=SENTINELS, sentinel_master_name="mymaster", db=1
        )
        connection = broker.config.broker_config.connection
        client = await connection.connect()
        try:
            assert isinstance(client.connection_pool, SentinelConnectionPool)
            assert client.connection_pool.service_name == "mymaster"
        finally:
            await connection.disconnect()


@pytest.mark.redis()
class TestRedisSentinelFastAPIRouterUnit:
    """RedisSentinelRouter (FastAPI) must build a Sentinel-backed broker."""

    def test_router_builds_sentinel_broker(self) -> None:
        from faststream.redis.fastapi import RedisSentinelRouter

        router = RedisSentinelRouter(
            sentinels=SENTINELS, sentinel_master_name="mymaster"
        )
        assert isinstance(router.broker, RedisSentinelBroker)
        connection = router.broker.config.broker_config.connection
        assert isinstance(connection, RedisSentinelConnectionState)
        assert connection._master_name == "mymaster"
        assert connection._sentinels == SENTINELS

    def test_router_requires_master_name(self) -> None:
        from faststream.redis.fastapi import RedisSentinelRouter

        with pytest.raises(SetupError, match="sentinel_master_name"):
            RedisSentinelRouter(sentinels=SENTINELS)
