import logging
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional

from fast_depends import Provider, dependency_provider
from typing_extensions import override

from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.message import gen_cor_id
from faststream.mq.configs import MQBrokerConfig
from faststream.mq.helpers import MQConnectionConfig
from faststream.mq.publisher.producer import AsyncMQFastProducerImpl
from faststream.mq.response import MQPublishCommand
from faststream.mq.schemas import MQQueue
from faststream.mq.security import parse_security
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec

from .logging import make_mq_logger_state
from .registrator import MQRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import LoggerProto, SendableMessage
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.mq.helpers.client import AsyncMQConnection
    from faststream.mq.message import MQMessage, MQRawMessage
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict


class MQBroker(
    MQRegistrator, BrokerUsecase["MQRawMessage", "AsyncMQConnection", MQBrokerConfig]
):
    def __init__(
        self,
        queue_manager: str = "QM1",
        *,
        channel: str = "DEV.APP.SVRCONN",
        conn_name: str | None = None,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        reply_model_queue: str = "DEV.APP.MODEL.QUEUE",
        wait_interval: float = 1.0,
        declare_queues: bool = False,
        admin_channel: str | None = None,
        admin_conn_name: str | None = None,
        admin_username: str | None = None,
        admin_password: str | None = None,
        graceful_timeout: float | None = None,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[MQRegistrator] = (),
        security: Optional["BaseSecurity"] = None,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "mqi",
        description: str | None = None,
        tags: Iterable["Tag | TagDict"] = (),
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
    ) -> None:
        security_args = parse_security(security)

        if conn_name is None:
            host = host or "127.0.0.1"
            port = port or 1414
            conn_name = f"{host}({port})"

        username = security_args.get("username") or username
        password = security_args.get("password") or password

        specification_url = specification_url or f"mq://{queue_manager}@{conn_name}"
        protocol = protocol or "ibmmq"

        super().__init__(
            routers=routers,
            config=MQBrokerConfig(
                connection_config=MQConnectionConfig(
                    queue_manager=queue_manager,
                    channel=channel,
                    conn_name=conn_name,
                    username=username,
                    password=password,
                    reply_model_queue=reply_model_queue,
                    wait_interval=wait_interval,
                    use_ssl=bool(security_args.get("use_ssl", False)),
                    ssl_context=security_args.get("ssl_context"),
                    declare_queues=declare_queues,
                    admin_channel=admin_channel,
                    admin_conn_name=admin_conn_name,
                    admin_username=admin_username,
                    admin_password=admin_password,
                ),
                producer=AsyncMQFastProducerImpl(
                    parser=parser,
                    decoder=decoder,
                ),
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                logger=make_mq_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
                extra_context={
                    "broker": self,
                },
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

    @override
    async def _connect(self) -> "AsyncMQConnection":
        await self.config.connect()
        assert self.config.producer.connection is not None
        return self.config.producer.connection

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

    @override
    async def ping(self, timeout: float | None = None) -> bool:
        if not self._producer:
            return False
        return await self._producer.ping(timeout or 5.0)

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        queue: MQQueue | str = "",
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        reply_to_qmgr: str = "",
        priority: int | None = None,
        persistence: bool | None = None,
        expiry: int | None = None,
        message_type: str | None = None,
        message_id: str | None = None,
    ) -> None:
        cmd = MQPublishCommand(
            message,
            destination=MQQueue.validate(queue).add_prefix(self.config.prefix).routing(),
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            reply_to=reply_to,
            reply_to_qmgr=reply_to_qmgr,
            priority=priority,
            persistence=persistence,
            expiry=expiry,
            message_type=message_type,
            message_id=message_id,
            _publish_type=PublishType.PUBLISH,
        )
        await super()._basic_publish(cmd, producer=self._producer)

    @override
    async def request(
        self,
        message: "SendableMessage" = None,
        queue: MQQueue | str = "",
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        reply_to_qmgr: str = "",
        priority: int | None = None,
        persistence: bool | None = None,
        expiry: int | None = None,
        message_type: str | None = None,
        message_id: str | None = None,
        timeout: float = 0.5,
    ) -> "MQMessage":
        cmd = MQPublishCommand(
            message,
            destination=MQQueue.validate(queue).add_prefix(self.config.prefix).routing(),
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            reply_to=reply_to,
            reply_to_qmgr=reply_to_qmgr,
            priority=priority,
            persistence=persistence,
            expiry=expiry,
            message_type=message_type,
            message_id=message_id,
            timeout=timeout,
            _publish_type=PublishType.PUBLISH,
        )
        return await super()._basic_request(cmd, producer=self._producer)
