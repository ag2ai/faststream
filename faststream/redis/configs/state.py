from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from typing import Any, Generic, TypeVar

from redis.asyncio.client import Redis
from redis.asyncio.cluster import RedisCluster
from redis.asyncio.connection import ConnectionPool
from redis.asyncio.sentinel import Sentinel

from faststream.__about__ import __version__
from faststream.exceptions import IncorrectState
from faststream.redis._compat import REDIS_V720, REDIS_V800

if REDIS_V720:
    from redis.driver_info import DriverInfo


ClientT = TypeVar("ClientT")


def _get_driver_info() -> dict[str, Any]:
    if REDIS_V720:
        return {
            "driver_info": DriverInfo(
                name="faststream",
                lib_version=__version__,
            )
        }
    return {
        "lib_name": "faststream",
        "lib_version": __version__,
    }


def _ensure_cluster_pubsub_supported() -> None:
    if not REDIS_V800:
        msg = (
            "RedisClusterBroker requires redis-py >= 8.0.0 for native async "
            "publish and pubsub support."
        )
        raise IncorrectState(msg)


class ConnectionState(ABC, Generic[ClientT]):
    """Base connection state."""

    def __init__(self, options: dict[str, Any] | None = None) -> None:
        self._options = options or {}

        self._connected = False
        self._client: ClientT | None = None

    @property
    def client(self) -> ClientT:
        if not self._client:
            msg = "Connection is not available yet. Please, connect the broker first."
            raise IncorrectState(msg)

        return self._client

    def __bool__(self) -> bool:
        return self._connected

    @abstractmethod
    async def connect(self) -> ClientT: ...

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()  # type: ignore[attr-defined]

        self._client = None
        self._connected = False


class RedisConnectionState(ConnectionState["Redis[bytes]"]):
    async def connect(self) -> "Redis[bytes]":
        connection_kwargs = self._options | _get_driver_info()

        pool = ConnectionPool(**connection_kwargs)
        client: Redis[bytes] = Redis.from_pool(pool)  # type: ignore[attr-defined]

        self._client = client
        self._connected = True

        return client


class RedisSentinelConnectionState(RedisConnectionState):
    """Builds the client via ``Sentinel.master_for`` for HA / failover.

    The underlying ``SentinelConnectionPool`` re-discovers the current master
    on every reconnect, so publishers and stream consumers fail over for free
    (both go through ``connection.client``).
    """

    def __init__(
        self,
        options: dict[str, Any] | None = None,
        *,
        sentinels: Sequence[tuple[str, int]],
        master_name: str,
        sentinel_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(options)
        self._sentinels = list(sentinels)
        self._master_name = master_name
        self._sentinel_kwargs = sentinel_kwargs

    async def connect(self) -> "Redis[bytes]":
        # ``host``/``port`` describe a single node and are meaningless for
        # Sentinel — the master address is discovered from the sentinels.
        connection_kwargs = {
            k: v for k, v in self._options.items() if k not in {"host", "port"}
        }
        connection_kwargs |= _get_driver_info()

        manager = Sentinel(
            self._sentinels,
            sentinel_kwargs=dict(self._sentinel_kwargs)
            if self._sentinel_kwargs is not None
            else None,
            **connection_kwargs,
        )
        client: Redis[bytes] = manager.master_for(self._master_name)

        self._client = client
        self._connected = True

        return client


class RedisClusterConnectionState(ConnectionState["RedisCluster[bytes]"]):
    """Manages a Redis Cluster connection using its native async client."""

    async def connect(self) -> "RedisCluster[bytes]":
        if self._connected:
            return self.client

        _ensure_cluster_pubsub_supported()

        connection_kwargs = {k: v for k, v in self._options.items() if v is not None}
        # Keep parser-facing replies RESP2-shaped across redis-py 8 (see issue #3009).
        connection_kwargs.setdefault("legacy_responses", True)
        connection_kwargs |= _get_driver_info()

        client: RedisCluster[bytes] = RedisCluster(**connection_kwargs)
        await client.initialize()

        self._client = client
        self._connected = True
        return client
