from datetime import datetime
import logging
from typing import Any, Iterable, Optional, Union, override
from fast_depends import Provider, dependency_provider
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from faststream._internal.context.repository import ContextRepo
from fast_depends.library.serializer import SerializerProto
from faststream._internal.basic_types import LoggerProto, SendableMessage
from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.di.config import FastDependsConfig
from faststream.security import BaseSecurity
from faststream.specification.schema.broker import BrokerSpec
from faststream.specification.schema.extra.tag import Tag, TagDict
from faststream.sqla.broker.registrator import SqlaRegistrator
from faststream.sqla.configs.broker import SqlaBrokerConfig
from faststream.sqla.broker.logging import make_sqla_logger_state
from faststream.sqla.publisher.producer import SqlaProducer
from faststream.sqla.response import SqlaPublishCommand


class SqlaBroker(
    SqlaRegistrator,
    BrokerUsecase[
        Any,
        Any,
    ],
):
    url: list[str]

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        # broker base args
        routers: Iterable[SqlaRegistrator] = (),
        # AsyncAPI args
        security: Optional["BaseSecurity"] = None,
        specification_url: str | Iterable[str] | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "auto",
        description: str | None = None,
        tags: Iterable[Union["Tag", "TagDict"]] = (),
        # logging args
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        # FastDepends args
        # apply_types: bool = True,
        # serializer: Optional["SerializerProto"] = EMPTY,
        # provider: Optional["Provider"] = None,
        # context: Optional["ContextRepo"] = None,
    ) -> None:

        super().__init__(
            routers=routers,
            config=SqlaBrokerConfig(
                producer=SqlaProducer(
                    engine=engine,
                ),
                logger=make_sqla_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
                # fd_config=FastDependsConfig(
                #     use_fastdepends=apply_types,
                #     serializer=serializer,
                #     provider=provider or dependency_provider,
                #     context=context or ContextRepo(),
                # ),
                extra_context={
                    "broker": self,
                },
            ),
            specification=BrokerSpec(
                description=description,
                url=specification_url,
                protocol=protocol,
                protocol_version=protocol_version,
                security=security,
                tags=tags,
            ),
        )
    
    async def _connect(self) -> Any:
        return True

    async def start(self) -> None:
        await self.connect()
        await super().start()
    
    @override
    async def publish(
        self,
        message: "SendableMessage",
        *,
        queue: str,
        next_attempt_at: datetime | None = None,
        connection: AsyncConnection | None = None,
    ) -> None:
        """
        Args:
            next_attempt_at: datetime with timezone
        """
        cmd = SqlaPublishCommand(
            message=message,
            queue=queue,
            next_attempt_at=next_attempt_at,
            connection=connection,
        )

        return await super()._basic_publish(cmd, producer=self.config.producer)