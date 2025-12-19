from __future__ import annotations

from abc import ABC, abstractmethod
import asyncio
import enum
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import os
import random
import signal
from time import perf_counter

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
    case,
)
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.ext.asyncio import AsyncConnection
from faststream.exceptions import FeatureNotSupportedException, SetupError
from faststream.sqla.message import SqlaMessage, SqlaMessageState


metadata = MetaData()


message = Table(
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
    Column("attempts_count", SmallInteger, nullable=False, default=0),
    Column("created_at", DateTime, nullable=False, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None)),
    Column("first_attempt_at", DateTime),
    Column(
        "next_attempt_at",
        DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        index=True,
    ),
    Column("last_attempt_at", DateTime),
    Column("acquired_at", DateTime),
)


message_archive = Table(
    "message_archive",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("queue", String(255), nullable=False, index=True),
    Column("payload", LargeBinary, nullable=False),
    Column("state", Enum(SqlaMessageState), nullable=False, index=True),
    Column("attempts_count", SmallInteger, nullable=False),
    Column("created_at", DateTime, nullable=False),
    Column("first_attempt_at", DateTime),
    Column("last_attempt_at", DateTime),
    Column("archived_at", DateTime, nullable=False, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None)),
)


_MESSAGE_SELECT_COLUMNS = (
    message.c.id.label("id"),
    message.c.queue.label("queue"),
    message.c.payload.label("payload"),
    message.c.state.label("state"),
    message.c.attempts_count.label("attempts_count"),
    message.c.created_at.label("created_at"),
    message.c.first_attempt_at.label("first_attempt_at"),
    message.c.next_attempt_at.label("next_attempt_at"),
    message.c.last_attempt_at.label("last_attempt_at"),
    message.c.acquired_at.label("acquired_at"),
)


class SqlaPostgresClient:
    def __init__(self, engine: AsyncEngine):
        self._engine = engine

    async def enqueue(
        self,
        payload: bytes,
        *,
        queue: str,
        next_attempt_at: datetime | None = None,
        connection: AsyncConnection | None = None,
    ) -> None:
        if next_attempt_at:
            stmt = insert(message).values(
                queue=queue,
                payload=payload,
                next_attempt_at=next_attempt_at,
            )
        else:
            stmt = insert(message).values(
                queue=queue,
                payload=payload,
            )
        
        if connection:
            await connection.execute(stmt)
        else:
            async with self._engine.begin() as conn:
                await conn.execute(stmt)

    async def fetch(
        self,
        queues: list[str],
        *,
        limit: int,
    ) -> list[SqlaMessage]:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        ready = (
            select(*_MESSAGE_SELECT_COLUMNS)
            .where(
                or_(
                    message.c.state == SqlaMessageState.PENDING,
                    message.c.state == SqlaMessageState.RETRYABLE,
                ),
                message.c.next_attempt_at <= now,
                or_(*(message.c.queue == queue for queue in queues)),
            )
            .order_by(message.c.next_attempt_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .cte("ready")
        )
        updated = (
            update(message)
            .where(message.c.id.in_(select(ready.c.id)))
            .values(
                state=SqlaMessageState.PROCESSING,
                attempts_count=message.c.attempts_count + 1,
                acquired_at=now,
                first_attempt_at=case(
                    (message.c.attempts_count == 0, now),
                    else_=message.c.first_attempt_at
                ),
                last_attempt_at=case(
                    (message.c.attempts_count == 0, now),
                    else_=message.c.last_attempt_at
                ),
            )
            .returning(message)
            .cte("updated")
        )
        stmt = select(updated).order_by(updated.c.next_attempt_at)
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt)
            return [SqlaMessage(**row) for row in result.mappings()]

    async def retry(self, messages: Sequence[SqlaMessage]) -> None:
        if not messages:
            return
        params = [
            {
                "message_id": message.id,
                "state": message.state,
                "attempts_count": message.attempts_count,
                "first_attempt_at": message.first_attempt_at,
                "next_attempt_at": message.next_attempt_at,
                "last_attempt_at": message.last_attempt_at,
            }
            for message in messages
        ]
        stmt = (
            update(message)
            .where(message.c.id == bindparam("message_id"))
            .values(
                state=bindparam("state"),
                first_attempt_at=bindparam("first_attempt_at"),
                next_attempt_at=bindparam("next_attempt_at"),
                last_attempt_at=bindparam("last_attempt_at"),
                acquired_at=None,
            )
        )
        async with self._engine.begin() as conn:
            print('retry', params)
            await conn.execute(stmt, params)

    async def archive(self, messages: Sequence[SqlaMessage]) -> None:
        if not messages:
            return
        async with self._engine.begin() as conn:
            values = [
                {
                    "id": item.id,
                    "queue": item.queue,
                    "payload": item.payload,
                    "state": item.state,
                    "attempts_count": item.attempts_count,
                    "created_at": item.created_at,
                    "first_attempt_at": item.first_attempt_at,
                    "last_attempt_at": item.last_attempt_at,
                }
                for item in messages
            ]
            stmt = message_archive.insert().values(values)
            print('archive', values)
            await conn.execute(stmt)
            delete_stmt = delete(message).where(message.c.id.in_([item.id for item in messages]))
            print('delete', messages)
            await conn.execute(delete_stmt)

    async def release_stuck(self, timeout: int) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        select_stuck = (
            select(message.c.id)
            .where(
                message.c.state == SqlaMessageState.PROCESSING,
                message.c.acquired_at < now - timedelta(seconds=timeout),
            )
        )
        stmt = (
            update(message)
            .where(message.c.id.in_(select_stuck))
            .values(
                state=SqlaMessageState.PENDING,
                next_attempt_at=now,
                acquired_at=None,
            )
        )
        async with self._engine.begin() as conn:
            await conn.execute(stmt)


class SqlaMySqlClient(SqlaPostgresClient):
    async def fetch(
        self,
        queues: list[str],
        *,
        limit: int,
    ) -> list[SqlaMessage]:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        
        async with self._engine.begin() as conn:
            ready_stmt = (
                select(message.c.id.label("id"))
                .where(
                    or_(
                        message.c.state == SqlaMessageState.PENDING,
                        message.c.state == SqlaMessageState.RETRYABLE,
                    ),
                    message.c.next_attempt_at <= now,
                    or_(*(message.c.queue == queue for queue in queues)),
                )
                .order_by(message.c.next_attempt_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )

            ready_result = await conn.execute(ready_stmt)
            ready_ids = ready_result.scalars().all()
            if not ready_ids:
                return []

            update_stmt = (
                update(message)
                .where(message.c.id.in_(ready_ids))
                .values(
                    state=SqlaMessageState.PROCESSING,
                    attempts_count=message.c.attempts_count + 1,
                    acquired_at=now,
                    first_attempt_at=case(
                        (message.c.attempts_count == 1, now), # diff from postgres
                        else_=message.c.first_attempt_at,
                    ),
                    last_attempt_at=case(
                        (message.c.attempts_count == 1, now), # diff from postgres
                        else_=message.c.last_attempt_at,
                    ),
                )
            )
            await conn.execute(update_stmt)

            fetch_stmt = (
                select(*_MESSAGE_SELECT_COLUMNS)
                .where(message.c.id.in_(ready_ids))
            )
            fetched = await conn.execute(fetch_stmt)
            rows = fetched.mappings().all()

        rows_by_id = {row["id"]: row for row in rows}
        ordered_rows = [rows_by_id[id_] for id_ in ready_ids if id_ in rows_by_id]
        return [SqlaMessage(**row) for row in ordered_rows]

    async def release_stuck(self, timeout: int) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        select_stuck = (
            select(message.c.id)
            .where(
                message.c.state == SqlaMessageState.PROCESSING,
                message.c.acquired_at < now - timedelta(seconds=timeout),
            )
            .subquery()
        )
        stmt = (
            update(message)
            .where(message.c.id.in_(select(select_stuck.c.id)))
            .values(
                state=SqlaMessageState.PENDING,
                next_attempt_at=now,
                acquired_at=None,
            )
        )
        async with self._engine.begin() as conn:
            await conn.execute(stmt)


def create_sqla_client(engine: AsyncEngine) -> SqlaPostgresClient:
    match engine.dialect.name.lower():
        case "mysql":
            return SqlaMySqlClient(engine)
        case "postgresql":
            return SqlaPostgresClient(engine)
        case _:
            raise FeatureNotSupportedException
