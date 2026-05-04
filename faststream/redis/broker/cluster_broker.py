import logging
import warnings
from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional, Union, cast
from urllib.parse import urlparse

import anyio
from fast_depends import Provider, dependency_provider
from redis.asyncio.cluster import ClusterNode
from redis.asyncio.connection import DefaultParser, Encoder
from typing_extensions import override

from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream._internal.utils.nuid import NUID
from faststream.middlewares import AckPolicy
from faststream.redis.broker import RedisBroker
from faststream.redis.broker.broker import _resolve_url_options
from faststream.redis.configs import ConnectionState, RedisBrokerConfig
from faststream.redis.configs.cluster import ClusterConnectionState
from faststream.redis.exceptions import UnreachablePathError
from faststream.redis.message import DATA_KEY
from faststream.redis.parser import BinaryMessageFormatV1, MessageFormat
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.response import DestinationType, RedisPublishCommand
from faststream.redis.subscriber.usecases.basic import LogicSubscriber
from faststream.specification.schema import BrokerSpec

from .logging import make_redis_logger_state
from .registrator import RedisRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from redis.asyncio.client import Pipeline
    from redis.asyncio.connection import BaseParser, Connection

    from faststream._internal.basic_types import LoggerProto, SendableMessage
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.redis.schemas import ListSub, PubSub, StreamSub
    from faststream.redis.subscriber.usecases import ChannelSubscriber
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict


_CLUSTER_INCOMPATIBLE_PARAMS = frozenset({
    "db",
    "socket_read_size",
    "socket_type",
    "retry_on_timeout",
    "parser_class",
    "encoder_class",
    "connection_class",
})


def _clean_cluster_options(
    raw: dict[str, Any],
    *,
    startup_nodes: list[tuple[str, int]] | None = None,
    host: str = EMPTY,
    port: str | int = EMPTY,
) -> dict[str, Any]:
    nodes: list[ClusterNode] = []

    parsed_host = raw.get("host")
    parsed_port = int(raw.get("port", 6379))

    if host is not EMPTY:
        parsed_host = str(host)
    if port is not EMPTY:
        parsed_port = int(port)

    if parsed_host:
        nodes.append(ClusterNode(parsed_host, parsed_port))
    if startup_nodes:
        for h, p in startup_nodes:
            nodes.append(ClusterNode(h, int(p)))

    cleaned = {
        k: v
        for k, v in raw.items()
        if k not in _CLUSTER_INCOMPATIBLE_PARAMS and k not in {"host", "port"}
    }
    cleaned["startup_nodes"] = nodes
    cleaned.pop("connection_class", None)
    return cleaned


class ClusterFastProducer(RedisFastProducer):
    """Producer that routes channel operations through the sync cluster."""

    def __init__(
        self,
        connection: "ConnectionState",
        cluster_state: ClusterConnectionState,
        **kwargs: Any,
    ) -> None:
        super().__init__(connection=connection, **kwargs)
        self._cluster_state = cluster_state

    @property
    def cluster_state(self) -> ClusterConnectionState:
        return self._cluster_state

    @override
    def _build_child(self, **kwargs: Any) -> "ClusterFastProducer":
        return ClusterFastProducer(cluster_state=self._cluster_state, **kwargs)

    @override
    async def publish(self, cmd: "RedisPublishCommand") -> int | bytes:
        msg = cmd.message_format.encode(
            message=cmd.body,
            reply_to=cmd.reply_to,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id or "",
            serializer=self.serializer,
        )

        if cmd.destination_type is DestinationType.Channel:
            return await self._cluster_state.sync_publish(cmd.destination, msg)

        # For List/Stream — use the async RedisCluster connection
        connection = cmd.pipeline or self._connection.client
        if cmd.destination_type is DestinationType.List:
            return await connection.rpush(cmd.destination, msg)
        if cmd.destination_type is DestinationType.Stream:
            return cast(
                "int | bytes",
                await connection.xadd(
                    name=cmd.destination,
                    fields={DATA_KEY: msg},
                    maxlen=cmd.maxlen,
                ),
            )
        raise UnreachablePathError

    @override
    async def request(self, cmd: "RedisPublishCommand") -> "Any":
        nuid = NUID()
        reply_to = str(nuid.next(), "utf-8")
        psub = self._cluster_state.pubsub()

        try:
            await psub.subscribe(reply_to)

            msg = cmd.message_format.encode(
                message=cmd.body,
                reply_to=reply_to,
                headers=cmd.headers,
                correlation_id=cmd.correlation_id or "",
                serializer=self.serializer,
            )

            if cmd.destination_type is DestinationType.Channel:
                await self._cluster_state.sync_publish(cmd.destination, msg)
            elif cmd.destination_type is DestinationType.List:
                await self._connection.client.rpush(cmd.destination, msg)
            elif cmd.destination_type is DestinationType.Stream:
                await self._connection.client.xadd(
                    name=cmd.destination,
                    fields={DATA_KEY: msg},
                    maxlen=cmd.maxlen,
                )
            else:
                raise UnreachablePathError

            with anyio.fail_after(cmd.timeout) as scope:
                # skip subscribe message
                await psub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=cmd.timeout or 0.0,
                )

                # get real response
                response_msg = await psub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=cmd.timeout or 0.0,
                )

            if scope.cancel_called:
                raise TimeoutError

            return response_msg

        finally:
            with suppress(Exception):
                await psub.unsubscribe()
                await psub.aclose()


class RedisClusterBroker(RedisBroker):
    def __init__(
        self,
        url: str = "redis://localhost:6379",
        *,
        host: str = EMPTY,
        port: str | int = EMPTY,
        db: str | int = EMPTY,
        connection_class: type["Connection"] = EMPTY,
        client_name: str | None = None,
        health_check_interval: float = 0,
        max_connections: int | None = None,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = None,
        socket_read_size: int = 65536,
        socket_keepalive: bool = False,
        socket_keepalive_options: Mapping[int, int | bytes] | None = None,
        socket_type: int = 0,
        retry_on_timeout: bool = False,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        parser_class: type["BaseParser"] = DefaultParser,
        encoder_class: type["Encoder"] = Encoder,
        graceful_timeout: float | None = 15.0,
        ack_policy: AckPolicy = EMPTY,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        parser: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[RedisRegistrator] = (),
        message_format: type["MessageFormat"] = BinaryMessageFormatV1,
        security: Optional["BaseSecurity"] = None,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "custom",
        description: str | None = None,
        tags: Iterable[Union["Tag", "TagDict"]] = (),
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
        startup_nodes: list[tuple[str, int]] | None = None,
    ) -> None:
        self.message_format = message_format

        if specification_url is None:
            specification_url = url
        if protocol is None:
            url_kwargs = urlparse(specification_url)
            protocol = url_kwargs.scheme

        all_options = _resolve_url_options(
            url,
            security=security,
            host=host,
            port=port,
            db=db,
            client_name=client_name,
            health_check_interval=health_check_interval,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_read_size=socket_read_size,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            socket_type=socket_type,
            retry_on_timeout=retry_on_timeout,
            encoding=encoding,
            encoding_errors=encoding_errors,
            parser_class=parser_class,
            connection_class=connection_class,
            encoder_class=encoder_class,
        )

        cluster_opts = _clean_cluster_options(
            all_options,
            startup_nodes=startup_nodes,
            host=host,
            port=port,
        )

        connection_state = ClusterConnectionState(cluster_opts)

        super(RedisBroker, self).__init__(
            **all_options,
            routers=routers,
            config=RedisBrokerConfig(
                connection=cast("ConnectionState", connection_state),
                producer=ClusterFastProducer(
                    connection=cast("ConnectionState", connection_state),
                    cluster_state=connection_state,
                    parser=parser,
                    decoder=decoder,
                    message_format=self.message_format,
                    serializer=serializer,
                ),
                message_format=self.message_format,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                broker_codec=codec,
                logger=make_redis_logger_state(logger=logger, log_level=log_level),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
                ack_policy=ack_policy,
                extra_context={"broker": self},
            ),
            specification=BrokerSpec(
                description=description,
                url=[specification_url],
                protocol=protocol,
                protocol_version=protocol_version,
                security=security,
                tags=tags,
            ),
        )

    @property
    def _cluster_state(self) -> ClusterConnectionState:
        return cast("ClusterConnectionState", self.config.broker_config.connection)

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
