from typing import Any

from redis.asyncio.client import Redis
from redis.asyncio.connection import ConnectionPool
from redis.driver_info import DriverInfo

from faststream.__about__ import __version__
from faststream.exceptions import IncorrectState


def _make_driver_info() -> DriverInfo:
    return DriverInfo().add_upstream_driver("faststream", __version__)


def _cluster_driver_kwargs() -> dict[str, Any]:
    """Return driver identification kwargs compatible with the installed redis-py.

    ``RedisCluster`` gained the ``driver_info`` parameter in redis-py 8.x.
    Older versions (7.x) only accept ``lib_name`` / ``lib_version``.
    """
    import inspect

    from redis.asyncio.cluster import RedisCluster

    if "driver_info" in inspect.signature(RedisCluster.__init__).parameters:
        return {"driver_info": _make_driver_info()}
    return {"lib_name": "faststream", "lib_version": __version__}


# Keys from RedisBroker.__init__ that RedisCluster does not accept.
_CLUSTER_UNSUPPORTED_KEYS: frozenset[str] = frozenset({
    "db",
    "connection_class",
    "socket_read_size",
    "socket_type",
    "retry_on_timeout",
    "parser_class",
    "encoder_class",
    "decode_responses",
})


class ConnectionState:
    def __init__(self, options: dict[str, Any] | None = None) -> None:
        self._options = options or {}

        self._connected = False
        self._client: Redis[bytes] | None = None

    @property
    def client(self) -> "Redis[bytes]":
        if not self._client:
            msg = "Connection is not available yet. Please, connect the broker first."
            raise IncorrectState(msg)

        return self._client

    def __bool__(self) -> bool:
        return self._connected

    async def connect(self) -> "Redis[bytes]":
        pool = ConnectionPool(
            **self._options,
            driver_info=_make_driver_info(),
        )
        client: Redis[bytes] = Redis.from_pool(pool)  # type: ignore[attr-defined]

        self._client = client
        self._connected = True

        return client

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()  # type: ignore[attr-defined]

        self._client = None
        self._connected = False


class ClusterConnectionState(ConnectionState):
    """Connection state backed by ``redis.asyncio.cluster.RedisCluster``."""

    def __init__(self, options: dict[str, Any] | None = None) -> None:
        raw = dict(options or {})

        # Strip keys that RedisCluster does not accept.
        for key in _CLUSTER_UNSUPPORTED_KEYS:
            raw.pop(key, None)

        # RedisCluster requires max_connections; default mirrors its own default.
        raw.setdefault("max_connections", 2**31)

        super().__init__(raw)

    async def connect(self) -> "Redis[bytes]":
        from redis.asyncio.cluster import RedisCluster

        client: RedisCluster[bytes] = RedisCluster(
            **self._options,
            **_cluster_driver_kwargs(),
        )

        self._client = client  # type: ignore[assignment]
        self._connected = True

        return client  # type: ignore[return-value]

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()  # type: ignore[attr-defined]

        self._client = None
        self._connected = False
