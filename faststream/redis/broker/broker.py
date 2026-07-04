import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    cast,
)
from urllib.parse import urlparse

import anyio
from anyio import move_on_after
from fast_depends import dependency_provider
from redis.asyncio.connection import (
    parse_url,
)
from redis.exceptions import ConnectionError
from typing_extensions import Unpack, overload, override

from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.message import gen_cor_id
from faststream.redis.configs import RedisBrokerConfig, RedisConnectionState
from faststream.redis.message import UnifyRedisDict
from faststream.redis.parser import BinaryMessageFormatV1
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.response import RedisPublishCommand
from faststream.redis.schemas.types import NON_CONNECTION_PARAMS
from faststream.redis.security import parse_security
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec

from .logging import make_redis_logger_state
from .registrator import RedisRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from redis.asyncio.client import Pipeline, Redis

    from faststream._internal.basic_types import SendableMessage
    from faststream.redis.message import RedisChannelMessage
    from faststream.redis.schemas.types import RedisBrokerParams
    from faststream.security import BaseSecurity


class RedisBroker(
    RedisRegistrator,
    BrokerUsecase[UnifyRedisDict, "Redis[bytes]"],
):
    """Redis broker."""

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        **kwargs: Unpack["RedisBrokerParams"],
    ) -> None:
        """Initialized the RedisBroker."""
        host = kwargs.pop("host", EMPTY)
        port = kwargs.pop("port", EMPTY)
        security = kwargs.pop("security", None)
        specification_url = kwargs.pop("specification_url", None)
        protocol = kwargs.pop("protocol", None)
        message_format = kwargs.pop("message_format", BinaryMessageFormatV1)

        self.message_format = message_format

        if specification_url is None:
            specification_url = url
        if protocol is None:
            protocol = urlparse(specification_url).scheme

        connection_kwargs = {
            k: v for k, v in kwargs.items() if k not in NON_CONNECTION_PARAMS
        }
        connection_options = self._resolve_url_options(
            url,
            security=security,
            host=host,
            port=port,
            **connection_kwargs,
        )

        connection_state = RedisConnectionState(connection_options)

        super().__init__(
            **connection_options,
            routers=kwargs.get("routers", ()),
            config=RedisBrokerConfig(
                connection=connection_state,
                producer=RedisFastProducer(
                    connection=connection_state,
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
                security=security,
                tags=kwargs.get("tags", ()),
            ),
        )

    @override
    async def _connect(self) -> "Redis[bytes]":
        await self.config.connect()
        return cast("Redis[bytes]", self.config.broker_config.connection.client)

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
        stream: None = None,
        maxlen: int | None = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
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
        stream: str = ...,
        maxlen: int | None = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> bytes: ...

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
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> int | bytes:
        """Publish message directly.

        This method allows you to publish a message in a non-AsyncAPI-documented way.
        It can be used in other frameworks or to publish messages at specific intervals.

        Args:
            message:
                Message body to send.
            channel:
                Redis PubSub object name to send message.
            reply_to:
                Reply message destination PubSub object name.
            headers:
                Message headers to store metainformation.
            correlation_id:
                Manual message correlation_id setter. correlation_id is a useful option to trace messages.
            list:
                Redis List object name to send message.
            stream:
                Redis Stream object name to send message.
            maxlen:
                Redis Stream maxlen publish option. Remove eldest message if maxlen exceeded.
            pipeline:
                Redis pipeline to use for publishing messages.

        Returns:
            int: The result of the publish operation, typically the number of messages published.
        """
        cmd = RedisPublishCommand(
            message,
            correlation_id=correlation_id or gen_cor_id(),
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

        result: int | bytes = await super()._basic_publish(
            cmd,
            producer=self.config.producer,
        )
        return result

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage",
        channel: str | None = None,
        *,
        list: str | None = None,
        stream: str | None = None,
        maxlen: int | None = None,
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | None = 30.0,
    ) -> "RedisChannelMessage":
        cmd = RedisPublishCommand(
            message,
            correlation_id=correlation_id or gen_cor_id(),
            channel=channel,
            list=list,
            stream=stream,
            maxlen=maxlen,
            headers=headers,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
            message_format=self.message_format,
        )
        msg: RedisChannelMessage = await super()._basic_request(
            cmd,
            producer=self.config.producer,
        )
        return msg

    @override
    async def publish_batch(  # type: ignore[override]
        self,
        *messages: "SendableMessage",
        list: str,
        correlation_id: str | None = None,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> int:
        """Publish multiple messages to Redis List by one request.

        Args:
            *messages: Messages bodies to send.
            list: Redis List object name to send messages.
            correlation_id: Manual message **correlation_id** setter. **correlation_id** is a useful option to trace messages.
            reply_to: Reply message destination PubSub object name.
            headers: Message headers to store metainformation.
            pipeline: Redis pipeline to use for publishing messages.

        Returns:
            int: The result of the batch publish operation.
        """
        cmd = RedisPublishCommand(
            *messages,
            list=list,
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            pipeline=pipeline,
            _publish_type=PublishType.PUBLISH,
            message_format=self.message_format,
        )

        result: int = await self._basic_publish_batch(
            cmd,
            producer=self.config.producer,
        )
        return result

    @override
    async def ping(self, timeout: float | None = 3) -> bool:
        sleep_time = (timeout or 10) / 10

        with move_on_after(timeout) as cancel_scope:
            if self._connection is None:
                return False

            while True:
                if cancel_scope.cancel_called:
                    return False

                try:
                    if await self._connection.ping():
                        return True

                except ConnectionError:
                    pass

                await anyio.sleep(sleep_time)

        return False

    @staticmethod
    def _resolve_url_options(
        url: str,
        *,
        security: Optional["BaseSecurity"],
        **kwargs: Any,
    ) -> dict[str, Any]:
        return {
            **dict(parse_url(url)),
            **parse_security(security),
            **{k: v for k, v in kwargs.items() if v is not EMPTY},
        }
