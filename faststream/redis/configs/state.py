from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from redis.asyncio.client import Redis
from redis.asyncio.cluster import RedisCluster
from redis.asyncio.connection import ConnectionPool

from faststream.__about__ import __version__
from faststream.exceptions import IncorrectState

ClientT = TypeVar("ClientT")


def _client_setinfo_kwargs() -> dict[str, Any]:
    """Return ``CLIENT SETINFO`` kwargs compatible with the installed redis-py.

    redis-py >= 7.4 exposes the ``driver_info`` parameter (and deprecates
    ``lib_name`` / ``lib_version``); older releases only accept the legacy
    pair. Importing ``redis.driver_info`` lazily keeps the ``redis>=5.0.0``
    floor working.
    """
    try:
        from redis.driver_info import DriverInfo
    except ImportError:  # redis-py < 7.4
        return {"lib_name": "faststream", "lib_version": __version__}
    return {"driver_info": DriverInfo().add_upstream_driver("faststream", __version__)}


def _ensure_cluster_pubsub_supported() -> None:
    """Ensure the installed redis-py supports async cluster pub/sub.

    The async ``RedisCluster`` only gained ``publish`` / ``pubsub`` in
    redis-py 8.0.0; on older versions cluster channels cannot work.
    """
    if not hasattr(RedisCluster, "pubsub"):
        msg = (
            "Redis Cluster support requires redis-py >= 8.0.0 for native async "
            "pub/sub. Please upgrade with: pip install 'redis>=8.0.0'."
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
        pool = ConnectionPool(
            **self._options,
            **_client_setinfo_kwargs(),
        )
        client: Redis[bytes] = Redis.from_pool(pool)  # type: ignore[attr-defined]

        self._client = client
        self._connected = True

        return client


class RedisClusterConnectionState(ConnectionState["RedisCluster[bytes]"]):
    """Manages a Redis Cluster connection lifecycle using the async client.

    redis-py >= 8.0.0 is required: the async ``RedisCluster`` exposes native
    ``publish`` / ``pubsub`` (cluster pub/sub) only from 8.0.0 onwards. The
    connection defaults to ``legacy_responses=True`` so RESP3 (the redis-8
    default) still yields RESP2-compatible response shapes.
    """

    async def connect(self) -> "RedisCluster[bytes]":
        if self._connected:
            return self._client  # type: ignore[return-value]

        _ensure_cluster_pubsub_supported()

        opts = {k: v for k, v in self._options.items() if v is not None}
        opts.setdefault("legacy_responses", True)
        opts.update(_client_setinfo_kwargs())

        client: RedisCluster[bytes] = RedisCluster(**opts)
        self._client = client
        self._connected = True
        return client
