import warnings
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Optional, cast

from redis.asyncio.cluster import ClusterNode
from redis.asyncio.connection import SSLConnection
from typing_extensions import Unpack

from faststream._internal.constants import EMPTY
from faststream.redis.broker import RedisBroker
from faststream.redis.configs.state import RedisClusterConnectionState
from faststream.redis.schemas.types import (
    CLUSTER_INCOMPATIBLE_PARAMS,
)

if TYPE_CHECKING:
    from types import TracebackType

    from redis.asyncio.client import Pipeline

    from faststream._internal.basic_types import SendableMessage
    from faststream.redis.schemas.types import RedisClusterParams
    from faststream.security import BaseSecurity


class RedisClusterBroker(RedisBroker):
    """A Redis Cluster broker."""

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        **kwargs: Unpack["RedisClusterParams"],
    ) -> None:
        self._init_broker(url, dict(kwargs))

    def _make_connection_state(
        self,
        connection_options: dict[str, Any],
        kwargs: dict[str, Any],
    ) -> RedisClusterConnectionState:
        return RedisClusterConnectionState(connection_options)

    @property
    def _cluster_state(self) -> RedisClusterConnectionState:
        return cast("RedisClusterConnectionState", self.config.broker_config.connection)

    async def publish(  # type: ignore[override]
        self,
        message: "SendableMessage" = None,
        channel: str | None = None,
        *,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        list: str | None = None,
        stream: str | None = None,
        maxlen: int | None = None,
        pipeline: Optional["Pipeline[bytes]"] = EMPTY,
    ) -> int | bytes:
        if pipeline is not EMPTY:
            warnings.warn(
                "Pipeline is not supported in Redis Cluster and will be ignored.",
                category=RuntimeWarning,
                stacklevel=2,
            )

        publish_kwargs: dict[str, Any] = {}
        if stream is not None:
            publish_kwargs["stream"] = stream
        if maxlen is not None:
            publish_kwargs["maxlen"] = maxlen

        if not self._cluster_state:
            await self._connect()

        return cast(
            "int | bytes",
            await super().publish(
                message,
                channel,
                reply_to=reply_to,
                headers=headers,
                correlation_id=correlation_id,
                list=list,
                **publish_kwargs,
            ),
        )

    async def _connect(self) -> Any:
        await self.config.connect()
        return self.config.broker_config.connection.client

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)
        await self.config.disconnect()
        self._connection = None

    async def start(self) -> None:
        await self.connect()
        await super().start()

    async def publish_batch(  # type: ignore[override]
        self,
        *messages: "SendableMessage",
        list: str,
        correlation_id: str | None = None,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        pipeline: Optional["Pipeline[bytes]"] = EMPTY,
    ) -> int:
        if pipeline is not EMPTY:
            warnings.warn(
                "Pipeline is not supported in Redis Cluster and will be ignored.",
                category=RuntimeWarning,
                stacklevel=2,
            )

        if not self._cluster_state:
            await self._connect()

        return await super().publish_batch(
            *messages,
            list=list,
            correlation_id=correlation_id,
            reply_to=reply_to,
            headers=headers,
        )

    @staticmethod
    def _resolve_url_options(
        url: str,
        *,
        startup_nodes: Iterable[tuple[str, int]] = (),
        host: str = EMPTY,
        port: str | int = EMPTY,
        security: Optional["BaseSecurity"] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        options = RedisBroker._resolve_url_options(
            url,
            security=security,
            host=host,
            port=port,
            **kwargs,
        )

        nodes: list[ClusterNode] = []
        cluster_host = str(host) if host is not EMPTY else options.get("host")
        cluster_port = int(port) if port is not EMPTY else int(options.get("port", 6379))
        if cluster_host:
            nodes.append(ClusterNode(cluster_host, cluster_port))
        for h, p in startup_nodes:
            nodes.append(ClusterNode(h, int(p)))

        # TLS is conveyed via `connection_class` (from `parse_security()` or a
        # `rediss://` URL), but RedisCluster doesn't accept it — translate to
        # its native `ssl` flag before the filter drops it.
        connection_class = options.get("connection_class")
        use_ssl = (security is not None and security.use_ssl) or (
            isinstance(connection_class, type)
            and issubclass(connection_class, SSLConnection)
        )

        result = {
            k: v for k, v in options.items() if k not in CLUSTER_INCOMPATIBLE_PARAMS
        } | {"startup_nodes": nodes}

        if use_ssl:
            result.setdefault("ssl", True)

            if security is not None and security.ssl_context is not None:
                warnings.warn(
                    "RedisCluster does not support a custom `ssl_context`, so it"
                    " will be ignored. Use `ssl_ca_certs`, `ssl_certfile`,"
                    " `ssl_keyfile` and other `ssl_*` connection options instead.",
                    category=RuntimeWarning,
                    stacklevel=3,
                )

        return result
