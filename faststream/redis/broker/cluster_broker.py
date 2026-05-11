import logging
import warnings
from typing import TYPE_CHECKING, Any, Optional, Union, cast
from urllib.parse import urlparse

from fast_depends import dependency_provider
from redis.asyncio.cluster import ClusterNode
from typing_extensions import Unpack

from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.redis.broker import RedisBroker
from faststream.redis.configs import RedisBrokerConfig
from faststream.redis.configs.state import RedisClusterConnectionState
from faststream.redis.parser import BinaryMessageFormatV1
from faststream.redis.publisher.producer import (
    RedisClusterFastProducer,
)
from faststream.redis.schemas.types import (
    CLUSTER_INCOMPATIBLE_PARAMS,
    NON_CONNECTION_PARAMS,
)
from faststream.redis.subscriber.usecases.basic import LogicSubscriber
from faststream.specification.schema import BrokerSpec

from .logging import make_redis_logger_state

if TYPE_CHECKING:
    from types import TracebackType

    from redis.asyncio.client import Pipeline

    from faststream._internal.basic_types import SendableMessage
    from faststream.redis.schemas import ListSub, PubSub, StreamSub
    from faststream.redis.schemas.types import RedisClusterParams
    from faststream.redis.subscriber.usecases import ChannelSubscriber
    from faststream.security import BaseSecurity


class RedisClusterBroker(RedisBroker):
    """A Redis Cluster broker."""

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        **kwargs: Unpack["RedisClusterParams"],
    ) -> None:
        startup_nodes = kwargs.get("startup_nodes")
        message_format = kwargs.pop("message_format", BinaryMessageFormatV1)
        specification_url = kwargs.pop("specification_url", None)
        protocol = kwargs.pop("protocol", None)
        self.message_format = message_format

        if specification_url is None:
            specification_url = url
        if protocol is None:
            protocol = urlparse(specification_url).scheme

        connection_options = self._resolve_url_options(
            url,
            startup_nodes=startup_nodes,
            host=kwargs.get("host", EMPTY),
            port=kwargs.get("port", EMPTY),
            security=kwargs.get("security"),
            **{
                k: v
                for k, v in kwargs.items()
                if k
                not in NON_CONNECTION_PARAMS
                | {"startup_nodes", "host", "port", "security"}
            },
        )

        connection_state = RedisClusterConnectionState(connection_options)

        super(RedisBroker, self).__init__(
            **connection_options,
            routers=kwargs.get("routers", ()),
            config=RedisBrokerConfig(
                connection=connection_state,
                producer=RedisClusterFastProducer(
                    connection=connection_state,
                    cluster_state=connection_state,
                    parser=kwargs.get("parser"),
                    decoder=kwargs.get("decoder"),
                    message_format=self.message_format,
                    serializer=kwargs.get("serializer"),
                ),
                message_format=self.message_format,
                broker_middlewares=kwargs.get("middlewares", ()),
                broker_parser=kwargs.get("parser"),
                broker_decoder=kwargs.get("decoder"),
                broker_codec=kwargs.get("codec"),
                logger=make_redis_logger_state(
                    logger=kwargs.get("logger", EMPTY),
                    log_level=kwargs.get("log_level", logging.INFO),
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=kwargs.get("apply_types", True),
                    serializer=kwargs.get("serializer", EMPTY),
                    provider=kwargs.get("provider") or dependency_provider,
                    context=kwargs.get("context") or ContextRepo(),
                ),
                broker_dependencies=kwargs.get("dependencies", ()),
                graceful_timeout=kwargs.get("graceful_timeout", 15.0),
                ack_policy=kwargs.get("ack_policy", EMPTY),
                extra_context={"broker": self},
            ),
            specification=BrokerSpec(
                description=kwargs.get("description"),
                url=[specification_url],
                protocol=protocol,
                protocol_version=kwargs.get("protocol_version", "custom"),
                security=kwargs.get("security"),
                tags=kwargs.get("tags", ()),
            ),
        )

    @property
    def _cluster_state(self) -> RedisClusterConnectionState:
        return cast("RedisClusterConnectionState", self.config.broker_config.connection)

    def subscriber(  # type: ignore[override]
        self,
        channel: Union["PubSub", str, None] = None,
        *,
        list: Union["ListSub", str, None] = None,
        stream: Union["StreamSub", str, None] = None,
        **kwargs: Any,
    ) -> "LogicSubscriber":
        if channel is not None:
            return self._make_channel_subscriber(channel, **kwargs)
        return super().subscriber(
            channel=None,
            list=list,
            stream=stream,
            **kwargs,
        )

    def _make_channel_subscriber(
        self,
        channel: Union["PubSub", str],
        **kwargs: Any,
    ) -> "ChannelSubscriber":
        state = self._cluster_state

        sub = cast(
            "ChannelSubscriber",
            super().subscriber(channel=channel, list=None, stream=None, **kwargs),
        )

        async def _patched_start() -> None:
            if sub.subscription:
                return
            psub = state.pubsub()
            sub.subscription = psub  # type: ignore[assignment]
            if sub.channel.pattern:
                await psub.psubscribe(sub.channel.name)
            else:
                await psub.subscribe(sub.channel.name)
            await LogicSubscriber.start(sub, psub)

        sub.start = _patched_start  # type: ignore[method-assign]
        return sub

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
        startup_nodes: list[tuple[str, int]] | None = None,
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
        if startup_nodes:
            for h, p in startup_nodes:
                nodes.append(ClusterNode(h, int(p)))

        return {
            k: v for k, v in options.items() if k not in CLUSTER_INCOMPATIBLE_PARAMS
        } | {"startup_nodes": nodes}
