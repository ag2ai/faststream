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
)
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

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


message_archive = Table(
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


class SqlaClient:
    def __init__(self, engine: AsyncEngine):
        self._engine = engine

    async def enqueue(
        self,
        queue: str,
        payload: bytes,
        *,
        next_attempt_at: datetime | None = None,
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
        async with self._engine.begin() as conn:
            await conn.execute(stmt)

    async def fetch(
        self,
        queue: str,
        *,
        limit: int,
    ) -> list[SqlaMessage]:
        ready = (
            select(
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
            .where(
                or_(
                    message.c.state ==SqlaMessageState.PENDING,
                    message.c.state == SqlaMessageState.RETRYABLE
                ),
                message.c.next_attempt_at <= func.now(),
                message.c.queue == queue,
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
                acquired_at=func.now(),
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
            await conn.execute(stmt)
            delete_stmt = delete(message).where(message.c.id.in_([item.id for item in messages]))
            await conn.execute(delete_stmt)

    async def release_stuck(self, timeout: int) -> None:
        select_stuck = (
            select(message.c.id)
            .where(
                message.c.state == SqlaMessageState.PROCESSING,
                message.c.acquired_at < datetime.now(timezone.utc) - timedelta(seconds=timeout),
            )
        )
        stmt = (
            update(message)
            .where(message.c.id.in_(select_stuck))
            .values(
                state=SqlaMessageState.PENDING,
                next_attempt_at=func.now(),
                acquired_at=None,
            )
        )
        async with self._engine.begin() as conn:
            await conn.execute(stmt)
