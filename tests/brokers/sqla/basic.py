from typing import Any, AsyncGenerator

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    Index,
    LargeBinary,
    MetaData,
    SmallInteger,
    String,
    Table,
    bindparam,
    delete,
    func,
    insert,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from faststream.sqla.broker.broker import SqlaBroker
from faststream.sqla.message import SqlaMessageState
from tests.brokers.base.basic import BaseTestcaseConfig
from tests.brokers.sqla.conftest import Settings


class SqlaTestcaseConfig(BaseTestcaseConfig):
    def get_broker(
        self,
        engine: AsyncEngine,
        **kwargs: Any,
    ) -> SqlaBroker:
        return SqlaBroker(engine=engine, **kwargs)

    def patch_broker(self, broker: SqlaBroker, **kwargs: Any) -> SqlaBroker:
        return broker

    def get_router(self, **kwargs: Any) -> None:
        raise NotImplementedError
