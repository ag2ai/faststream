from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Protocol, cast, runtime_checkable

from redis.asyncio.cluster import RedisCluster
from redis.cluster import (
    ClusterNode,
    RedisCluster as SyncRC,
)

from faststream.__about__ import __version__
from faststream._internal.utils.functions import run_in_executor
from faststream.exceptions import IncorrectState


@runtime_checkable
class _ClusterClient(Protocol):
    """Async ``RedisCluster`` commands used by our code."""

    async def info(self, section: str = "") -> dict[str, Any]: ...

    async def aclose(self) -> None: ...

    async def ping(self) -> bool: ...

    async def publish(self, channel: str, message: bytes) -> int: ...


class _ClusterPublishProxy:
    def __init__(
        self,
        async_client: object,
        sync_publish_fn: Any,
    ) -> None:
        self._async = async_client
        self._sync_publish = sync_publish_fn

    async def publish(self, channel: str, message: bytes) -> int:
        return cast("int", await self._sync_publish(channel, message))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._async, name)


class _SyncPubSubProxy:
    """Wraps a **sync** ``redis.cluster.RedisCluster.pubsub()`` for async use.

    ``redis-py``'s async ``RedisCluster`` lacks ``publish`` / ``pubsub``
    until version 8.0.0.  We use the sync client via a
    ``ThreadPoolExecutor`` (following the same pattern as the Confluent
    adapter) so Pub/Sub works with ``redis-py >= 7.4.0``.
    """

    def __init__(self, sync_cluster: Any, pool: ThreadPoolExecutor) -> None:
        self._pool = pool
        self._psub = sync_cluster.pubsub()

    async def subscribe(self, channel: str) -> None:
        await run_in_executor(self._pool, self._psub.subscribe, channel)

    async def psubscribe(self, pattern: str) -> None:
        await run_in_executor(self._pool, self._psub.psubscribe, pattern)

    async def unsubscribe(self) -> None:
        await run_in_executor(self._pool, self._psub.unsubscribe)

    async def get_message(
        self,
        ignore_subscribe_messages: bool = False,
        timeout: float | None = 0.0,
    ) -> Any:
        return await run_in_executor(
            self._pool,
            partial(self._psub.get_message, ignore_subscribe_messages, timeout),
        )

    async def aclose(self) -> None:
        await run_in_executor(self._pool, self._psub.close)


class ClusterConnectionState:
    """Manages a Redis Cluster connection lifecycle.

    Uses an **async** ``RedisCluster`` for List/Stream/KV commands and a
    **sync** ``redis.cluster.RedisCluster`` (wrapped via
    ``run_in_executor``) for Pub/Sub — the async client doesn't expose
    ``publish`` / ``pubsub`` until ``redis-py >= 8.0.0``.
    """

    def __init__(self, options: dict[str, Any] | None = None) -> None:
        self._options = options or {}

        self._connected = False
        self._client: _ClusterClient | None = None
        self._sync_cluster: Any = None
        self._thread_pool: ThreadPoolExecutor | None = None

    @property
    def client(self) -> "_ClusterClient":
        if not self._client:
            msg = "Connection is not available yet. Please, connect the broker first."
            raise IncorrectState(msg)
        return self._client

    def __bool__(self) -> bool:
        return self._connected

    async def connect(self) -> "_ClusterClient":
        from typing import cast

        opts = {k: v for k, v in self._options.items() if v is not None}
        opts["lib_name"] = "faststream"
        opts["lib_version"] = __version__

        client = cast("_ClusterClient", RedisCluster(**opts))
        self._client = client
        self._connected = True
        return client

    async def disconnect(self) -> None:
        if self._thread_pool is not None:
            self._thread_pool.shutdown(wait=False)
            self._thread_pool = None
            self._sync_cluster = None
        if self._client:
            await self._client.aclose()
        self._client = None
        self._connected = False

    def _get_sync_cluster(self) -> Any:
        if self._sync_cluster is not None:
            return self._sync_cluster

        raw = self._options
        nodes = [ClusterNode(n.host, n.port) for n in raw.get("startup_nodes", [])]
        if not nodes:
            host = raw.get("host", "127.0.0.1")
            port = int(raw.get("port", 6379))
            nodes.append(ClusterNode(host, port))

        self._sync_cluster = SyncRC(
            startup_nodes=nodes,
            password=raw.get("password"),
            username=raw.get("username"),
            ssl=raw.get("ssl", False),
            socket_timeout=raw.get("socket_timeout"),
            socket_connect_timeout=raw.get("socket_connect_timeout"),
        )
        self._thread_pool = ThreadPoolExecutor(max_workers=4)
        return self._sync_cluster

    async def sync_publish(self, channel: str, body: bytes) -> int:
        sync = self._get_sync_cluster()
        return await run_in_executor(self._thread_pool, sync.publish, channel, body)

    def pubsub(self) -> _SyncPubSubProxy:
        sync = self._get_sync_cluster()
        if self._thread_pool is None:
            msg = "Pub/Sub proxy is not available"
            raise IncorrectState(msg)
        return _SyncPubSubProxy(sync, self._thread_pool)
