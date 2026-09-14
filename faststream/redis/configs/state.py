from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from typing import Any, Generic, TypeVar

from redis.asyncio.client import Redis
from redis.asyncio.cluster import RedisCluster
from redis.asyncio.connection import ConnectionPool
from redis.asyncio.sentinel import Sentinel
from redis.driver_info import DriverInfo

from faststream.__about__ import __version__
from faststream.exceptions import IncorrectState

ClientT = TypeVar("ClientT")


def _get_driver_info() -> dict[str, Any]:
    return {
        "driver_info": DriverInfo(
            name="faststream",
            lib_version=__version__,
        )
    }


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
    """Manages a Redis Cluster connection lifecycle.

    The async ``RedisCluster`` serves every command family — Channels, Lists,
    Streams and KV — since ``redis-py`` 8.0.0 gave it ``publish`` / ``pubsub``.
    """

    async def connect(self) -> "RedisCluster[bytes]":
        if self._connected:
            return self.client

        connection_kwargs = {k: v for k, v in self._options.items() if v is not None}
        connection_kwargs |= _get_driver_info()

        client: RedisCluster[bytes] = RedisCluster(**connection_kwargs)

        # `ClusterPubSub` reads the slot map directly instead of going through
        # `execute_command`, so it can't rely on the client's lazy discovery.
        await client.initialize()

        self._client = client
        self._connected = True

        return client
