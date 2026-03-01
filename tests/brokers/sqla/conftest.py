from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest_asyncio
from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Enum,
    Integer,
    LargeBinary,
    MetaData,
    SmallInteger,
    String,
    Table,
    text,
)
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from faststream.sqla.message import SqlaMessageState

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    import pytest


BACKENDS = (
    "postgresql",
    "mysql",
    "sqlite",
)  # fmt: skip


@pytest_asyncio.fixture
async def worker_id() -> str:
    return os.environ.get("PYTEST_XDIST_WORKER", "main")


@pytest_asyncio.fixture(params=BACKENDS)
async def master_engine(
    request: pytest.FixtureRequest,
    worker_id: str,
    tmp_path_factory: pytest.TempPathFactory,
) -> AsyncGenerator[AsyncEngine, None]:
    backend = request.param
    match backend:
        case "postgresql":
            url = "postgresql+asyncpg://broker:brokerpass@localhost:5432/broker"  # pragma: allowlist secret
        case "mysql":
            url = "mysql+asyncmy://broker:brokerpass@localhost:3306/broker"  # pragma: allowlist secret
        case "sqlite":
            db_root = tmp_path_factory.mktemp(f"sqla-{worker_id}")
            url = f"sqlite+aiosqlite:///{db_root / 'broker.db'}"
        case _:
            raise ValueError

    engine = create_async_engine(
        url,
    )  # fmt: skip

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def engine(
    master_engine: AsyncEngine, worker_id: str
) -> AsyncGenerator[AsyncEngine, None]:
    if master_engine.dialect.name == "sqlite":
        yield master_engine
        return

    async with master_engine.connect() as conn:
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        match master_engine.dialect.name:
            case "postgresql":
                result = await conn.execute(
                    text("SELECT 1 FROM pg_database WHERE datname = :database"),
                    {"database": worker_id},
                )
                if not result.scalar():
                    await conn.execute(text(f"CREATE DATABASE {worker_id}"))
                url = f"postgresql+asyncpg://broker:brokerpass@localhost:5432/{worker_id}"  # pragma: allowlist secret
            case "mysql":
                await conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {worker_id}"))
                url = f"mysql+asyncmy://broker:brokerpass@localhost:3306/{worker_id}"  # pragma: allowlist secret
            case _:
                raise ValueError

    engine = create_async_engine(
        url,
    )  # fmt: skip

    try:
        yield engine
    finally:
        await engine.dispose()


@dataclass
class Settings:
    engine: AsyncEngine


@pytest_asyncio.fixture
async def recreate_tables(engine: AsyncEngine) -> None:
    match engine.dialect.name:
        case "postgresql":
            timestamp_type = postgresql.TIMESTAMP(precision=3)
            json_type = postgresql.JSONB
            pk_type = BigInteger
        case "mysql":
            timestamp_type = mysql.TIMESTAMP(fsp=3)
            json_type = mysql.JSON
            pk_type = BigInteger
        case "sqlite":
            timestamp_type = DateTime
            json_type = JSON
            pk_type = BigInteger().with_variant(Integer, "sqlite")
        case _:
            raise ValueError

    metadata = MetaData()

    message = Table(  # noqa: F841
        "message",
        metadata,
        Column("id", pk_type, primary_key=True),
        Column("queue", String(255), nullable=False, index=True),
        Column("headers", json_type, nullable=True),
        Column("payload", LargeBinary, nullable=False),
        Column(
            "state",
            Enum(SqlaMessageState),
            nullable=False,
            index=True,
            server_default=SqlaMessageState.PENDING.name,
        ),
        Column("attempts_count", SmallInteger, nullable=False, default=0),
        Column("deliveries_count", SmallInteger, nullable=False, default=0),
        Column(
            "created_at",
            timestamp_type,
            nullable=False,
            default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        ),
        Column("first_attempt_at", timestamp_type),
        Column(
            "next_attempt_at",
            timestamp_type,
            nullable=False,
            default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
            index=True,
        ),
        Column("last_attempt_at", timestamp_type),
        Column("acquired_at", timestamp_type),
    )

    message_archive = Table(  # noqa: F841
        "message_archive",
        metadata,
        Column("id", pk_type, primary_key=True),
        Column("queue", String(255), nullable=False, index=True),
        Column("headers", json_type, nullable=True),
        Column("payload", LargeBinary, nullable=False),
        Column("state", Enum(SqlaMessageState), nullable=False, index=True),
        Column("attempts_count", SmallInteger, nullable=False),
        Column("deliveries_count", SmallInteger, nullable=False),
        Column("created_at", timestamp_type, nullable=False),
        Column("first_attempt_at", timestamp_type),
        Column("last_attempt_at", timestamp_type),
        Column(
            "archived_at",
            timestamp_type,
            nullable=False,
            default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        ),
    )

    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all, checkfirst=True)
        await conn.run_sync(metadata.create_all)


@pytest_asyncio.fixture
async def settings(engine: AsyncEngine, recreate_tables: None) -> Settings:
    return Settings(engine=engine)


def as_datetime(value: datetime | str) -> datetime:
    match value:
        case datetime():
            return value
        case str():
            return datetime.fromisoformat(value)
        case _:
            raise ValueError
