from typing import Any, AsyncGenerator, Generator

import pytest
import pytest_asyncio
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

from faststream.sqla.message import SqlaMessageState

from dataclasses import dataclass

import pytest


@dataclass
class Settings:
    url: str = "postgresql+asyncpg://broker:brokerpass@localhost:5432/broker"


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest_asyncio.fixture
async def engine(settings: Settings) -> AsyncGenerator[AsyncEngine, None]:
    engine = create_async_engine(settings.url, echo=True)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def recreate_tables(engine: AsyncEngine) -> None:
    metadata = MetaData()

    Table(
        "message",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("queue", String(255), nullable=False, index=True),
        Column("payload", LargeBinary, nullable=False),
        Column(
            "state",
            Enum(SqlaMessageState),
            nullable=False,
            index=True,
            server_default=SqlaMessageState.PENDING.name,
        ),
        Column("attempts_count", SmallInteger, nullable=False, server_default="0"),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        Column("first_attempt_at", DateTime(timezone=True)),
        Column(
            "next_attempt_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
            index=True,
        ),
        Column("last_attempt_at", DateTime(timezone=True)),
        Column("acquired_at", DateTime(timezone=True)),
    )

    Table(
        "message_archive",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("queue", String(255), nullable=False, index=True),
        Column("payload", LargeBinary, nullable=False),
        Column("state", Enum(SqlaMessageState), nullable=False, index=True),
        Column("attempts_count", SmallInteger, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("first_attempt_at", DateTime(timezone=True)),
        Column("last_attempt_at", DateTime(timezone=True)),
        Column(
            "archived_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
        ),
    )

    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS message CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS message_archive CASCADE"))
        await conn.run_sync(metadata.create_all)
        