import warnings
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Optional, cast, overload

from redis.asyncio.cluster import ClusterNode, ClusterPipeline
from redis.asyncio.connection import SSLConnection
from typing_extensions import Unpack, override

from faststream import PublishType
from faststream._internal.constants import EMPTY
from faststream.redis.broker import RedisBroker
from faststream.redis.configs.state import (
    ConnectionState,
    RedisClusterConnectionState,
)
from faststream.redis.response import RedisPublishCommand
from faststream.redis.schemas.types import (
    CLUSTER_INCOMPATIBLE_PARAMS,
)

if TYPE_CHECKING:
    from types import TracebackType

    from faststream._internal.basic_types import SendableMessage
    from faststream.redis.schemas.types import RedisClusterParams
    from faststream.security import BaseSecurity


class RedisClusterBroker(RedisBroker[ClusterPipeline]):
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
    ) -> "ConnectionState[Any]":
        return RedisClusterConnectionState(connection_options)

    @property
    def _cluster_state(self) -> RedisClusterConnectionState:
        return cast("RedisClusterConnectionState", self.config.broker_config.connection)

    @overload  # type: ignore[override]
    async def publish(
        self,
        message: "SendableMessage" = None,
        channel: str | None = None,
        *,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        list: str | None = None,
        stream: None = None,
        maxlen: int | None = None,
        pipeline: None = None,
    ) -> int: ...

    @overload
    async def publish(
        self,
        message: "SendableMessage" = None,
        channel: str | None = None,
        *,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        list: str | None = None,
        stream: str,
        maxlen: int | None = None,
        pipeline: None = None,
    ) -> bytes: ...

    @overload
    async def publish(
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
        pipeline: ClusterPipeline,
    ) -> ClusterPipeline: ...

    @override
    async def publish(
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
        pipeline: ClusterPipeline | None = None,
    ) -> int | bytes | ClusterPipeline:
        cmd = RedisPublishCommand(
            message,
            correlation_id=correlation_id or self.config.id_generator(),
            channel=channel,
            list=list,
            stream=stream,
            maxlen=maxlen,
            reply_to=reply_to,
            headers=headers,
            pipeline=pipeline,
            _publish_type=PublishType.PUBLISH,
            message_format=self.message_format,
        )

        result: int | bytes | ClusterPipeline = await super()._basic_publish(
            cmd,
            producer=self.config.producer,
        )
        return result

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

    @overload  # type: ignore[override]
    async def publish_batch(
        self,
        *messages: "SendableMessage",
        list: str,
        correlation_id: str | None = None,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        pipeline: None = None,
    ) -> int: ...

    @overload
    async def publish_batch(
        self,
        *messages: "SendableMessage",
        list: str,
        correlation_id: str | None = None,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        pipeline: ClusterPipeline,
    ) -> ClusterPipeline: ...

    @override
    async def publish_batch(
        self,
        *messages: "SendableMessage",
        list: str,
        correlation_id: str | None = None,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        pipeline: ClusterPipeline | None = None,
    ) -> int | ClusterPipeline:
        cmd = RedisPublishCommand(
            *messages,
            list=list,
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id or self.config.id_generator(),
            pipeline=pipeline,
            _publish_type=PublishType.PUBLISH,
            message_format=self.message_format,
        )

        result: int | ClusterPipeline = await self._basic_publish_batch(
            cmd,
            producer=self.config.producer,
        )
        return result

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
