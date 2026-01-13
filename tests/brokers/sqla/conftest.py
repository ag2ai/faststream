from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Enum,
    LargeBinary,
    MetaData,
    SmallInteger,
    String,
    Table,
    func,
    text,
)
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from faststream.sqla.message import SqlaMessageState


@pytest_asyncio.fixture(
    params=[
        "postgresql",
        "mysql"
    ]
)
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[AsyncEngine, None]:
    backend = request.param
    match backend:
        case "postgresql":
            url = "postgresql+asyncpg://broker:brokerpass@localhost:5432/broker"
        case "mysql":
            url = "mysql+asyncmy://broker:brokerpass@localhost:3306/broker"
        case _:
            raise ValueError

    engine = create_async_engine(url)

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def recreate_tables(engine: AsyncEngine) -> None:
    match engine.dialect.name:
        case "postgresql":
            timestamp_type = postgresql.TIMESTAMP(precision=3)
            json_type = postgresql.JSONB
        case "mysql":
            timestamp_type = mysql.TIMESTAMP(fsp=3)
            json_type = mysql.JSON
        case _:
            raise ValueError

    metadata = MetaData()

    message = Table(
        "message",
        metadata,
        Column("id", BigInteger, primary_key=True),
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
        Column("created_at", timestamp_type, nullable=False, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None)),
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


    message_archive = Table(
        "message_archive",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("queue", String(255), nullable=False, index=True),
        Column("headers", json_type, nullable=True),
        Column("payload", LargeBinary, nullable=False),
        Column("state", Enum(SqlaMessageState), nullable=False, index=True),
        Column("attempts_count", SmallInteger, nullable=False),
        Column("deliveries_count", SmallInteger, nullable=False),
        Column("created_at", timestamp_type, nullable=False),
        Column("first_attempt_at", timestamp_type),
        Column("last_attempt_at", timestamp_type),
        Column("archived_at", timestamp_type, nullable=False, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None)),
    )

    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all, checkfirst=True)
        await conn.run_sync(metadata.create_all)
