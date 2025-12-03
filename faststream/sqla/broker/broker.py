import logging
from typing import Any, Iterable, Optional, Union

from sqlalchemy.ext.asyncio import AsyncEngine

from faststream._internal.basic_types import LoggerProto
from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream.security import BaseSecurity
from faststream.specification.schema.broker import BrokerSpec
from faststream.specification.schema.extra.tag import Tag, TagDict
from faststream.sqla.broker.registrator import SqlaRegistrator
from faststream.sqla.configs.broker import SqlaBrokerConfig
from faststream.sqla.broker.logging import make_sqla_logger_state


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
    ) -> None:

        super().__init__(
            routers=routers,
            config=SqlaBrokerConfig(
                logger=make_sqla_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
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