from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

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
    bindparam,
    delete,
    insert,
    inspect,
    or_,
    select,
    text,
    update,
)

from faststream.exceptions import FeatureNotSupportedException, SetupError
from faststream.sqla.message import SqlaInnerMessage, SqlaMessageState

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy import Connection
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

metadata = MetaData()


message = Table(
    "message",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("queue", String(255), nullable=False, index=True),
    Column("headers", JSON, nullable=True),
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
        DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
    ),
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
    Column("headers", JSON, nullable=True),
    Column("payload", LargeBinary, nullable=False),
    Column("state", Enum(SqlaMessageState), nullable=False, index=True),
    Column("attempts_count", SmallInteger, nullable=False),
    Column("deliveries_count", SmallInteger, nullable=False),
    Column("created_at", DateTime, nullable=False),
    Column("first_attempt_at", DateTime),
    Column("last_attempt_at", DateTime),
    Column(
        "archived_at",
        DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
    ),
)


_MESSAGE_SELECT_COLUMNS = (
    message.c.id.label("id"),
    message.c.queue.label("queue"),
    message.c.headers.label("headers"),
    message.c.payload.label("payload"),
    message.c.state.label("state"),
    message.c.attempts_count.label("attempts_count"),
    message.c.deliveries_count.label("deliveries_count"),
    message.c.created_at.label("created_at"),
    message.c.first_attempt_at.label("first_attempt_at"),
    message.c.next_attempt_at.label("next_attempt_at"),
    message.c.last_attempt_at.label("last_attempt_at"),
    message.c.acquired_at.label("acquired_at"),
)


class SqlaBaseClient:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._schema_validator = SchemaValidator()

    async def enqueue(
        self,
        payload: bytes,
        *,
        queue: str,
        headers: dict[str, str] | None = None,
        next_attempt_at: datetime | None = None,
        connection: AsyncConnection | None = None,
    ) -> None:
        if next_attempt_at:
            stmt = (
                insert(message)
                .values(
                    queue=queue,
                    payload=payload,
                    headers=headers,
                    next_attempt_at=next_attempt_at,
                )
            )  # fmt: skip
        else:
            stmt = (
                insert(message)
                .values(
                    queue=queue,
                    payload=payload,
                    headers=headers,
                )
            )  # fmt: skip

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
    ) -> list[SqlaInnerMessage]:
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
                deliveries_count=message.c.deliveries_count + 1,
                acquired_at=now,
            )
            .returning(message)
            .cte("updated")
        )
        stmt = select(updated).order_by(updated.c.next_attempt_at)
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt)
            return [SqlaInnerMessage(**row) for row in result.mappings()]

    async def retry(self, messages: Sequence[SqlaInnerMessage]) -> None:
        if not messages:
            return
        params = [
            {
                "message_id": message.id,
                "state": message.state,
                "attempts_count": message.attempts_count,
                "deliveries_count": message.deliveries_count,
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
                attempts_count=bindparam("attempts_count"),
                deliveries_count=bindparam("deliveries_count"),
                first_attempt_at=bindparam("first_attempt_at"),
                next_attempt_at=bindparam("next_attempt_at"),
                last_attempt_at=bindparam("last_attempt_at"),
                acquired_at=None,
            )
        )
        async with self._engine.begin() as conn:
            await conn.execute(stmt, params)

    async def archive(self, messages: Sequence[SqlaInnerMessage]) -> None:
        if not messages:
            return
        async with self._engine.begin() as conn:
            values = [
                {
                    "id": msg.id,
                    "queue": msg.queue,
                    "payload": msg.payload,
                    "headers": msg.headers,
                    "state": msg.state,
                    "attempts_count": msg.attempts_count,
                    "deliveries_count": msg.deliveries_count,
                    "created_at": msg.created_at,
                    "first_attempt_at": msg.first_attempt_at,
                    "last_attempt_at": msg.last_attempt_at,
                }
                for msg in messages
            ]
            stmt = message_archive.insert().values(values)
            await conn.execute(stmt)
            delete_stmt = (
                delete(message)
                .where(
                    message.c.id.in_([item.id for item in messages])
                )
            )  # fmt: skip
            await conn.execute(delete_stmt)

    async def release_stuck(self, timeout: float) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        select_stuck = (
            select(message.c.id)
            .where(
                message.c.state == SqlaMessageState.PROCESSING,
                message.c.acquired_at < now - timedelta(seconds=timeout),
            )
        )  # fmt: skip
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

    async def validate_schema(self) -> None:
        async with self._engine.connect() as conn:
            errors = await conn.run_sync(self._schema_validator)
            if errors:
                msg = f"Schema validation failed: {'; '.join(errors)}"
                raise SetupError(msg)

    async def ping(self) -> bool:
        try:
            async with self._engine.connect() as conn:
                (await conn.execute(text("SELECT 1"))).scalar()
        except Exception:
            return False
        return True


class SqlaPostgresClient(SqlaBaseClient): ...


class SqlaMySqlClient(SqlaPostgresClient):
    async def fetch(
        self,
        queues: list[str],
        *,
        limit: int,
    ) -> list[SqlaInnerMessage]:
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
                    deliveries_count=message.c.deliveries_count + 1,
                    acquired_at=now,
                )
            )
            await conn.execute(update_stmt)

            fetch_stmt = (
                select(*_MESSAGE_SELECT_COLUMNS)
                .where(message.c.id.in_(ready_ids))
            )  # fmt: skip
            fetched_result = await conn.execute(fetch_stmt)
            rows = fetched_result.mappings().all()

        rows_by_id = {row["id"]: row for row in rows}
        ordered_rows = [rows_by_id[id_] for id_ in ready_ids if id_ in rows_by_id]
        return [SqlaInnerMessage(**row) for row in ordered_rows]

    async def release_stuck(self, timeout: float) -> None:
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


class SchemaValidator:
    def __call__(self, connection: Connection) -> list[str]:
        insp = inspect(connection)
        errors: list[str] = []

        for table_def in (message, message_archive):
            table_name = table_def.name
            if not insp.has_table(table_name):
                errors.append(f"Table '{table_name}' does not exist")
                continue

            db_columns = {c["name"]: c["type"] for c in insp.get_columns(table_name)}
            expected_columns = {c.name: c.type for c in table_def.columns}

            missing = set(expected_columns.keys()) - set(db_columns.keys())
            if missing:
                errors.append(f"Table '{table_name}' missing columns: {missing}")

            for col_name, expected_type in expected_columns.items():
                if col_name not in db_columns:
                    continue
                db_type = db_columns[col_name]
                if not self._types_compatible(expected_type, db_type):
                    errors.append(
                        f"Table '{table_name}' column '{col_name}' has type "
                        f"{type(db_type).__name__}, expected {type(expected_type).__name__}"
                    )

        return errors

    def _types_compatible(self, expected: Any, actual: Any) -> bool:
        from sqlalchemy.dialects.postgresql import JSONB
        from sqlalchemy.types import (
            BLOB,
            JSON,
            TIMESTAMP,
            VARCHAR,
            BigInteger,
            DateTime,
            Integer,
            LargeBinary,
            SmallInteger,
            String,
            Text,
            TypeDecorator,
        )

        if isinstance(expected, TypeDecorator):
            expected = expected.impl

        integer_types = (BigInteger, SmallInteger, Integer)
        string_types = (String, Text, VARCHAR)
        datetime_types = (DateTime, TIMESTAMP)
        binary_types = (LargeBinary, BLOB)
        json_types = (JSON, JSONB)

        for type_group in (
            integer_types,
            string_types,
            datetime_types,
            binary_types,
            json_types,
        ):
            if isinstance(expected, type_group) and isinstance(actual, type_group):
                return True

        if isinstance(expected, Enum) and isinstance(actual, Enum):
            return True

        return type(expected) is type(actual)


def create_sqla_client(engine: AsyncEngine) -> SqlaPostgresClient:
    match engine.dialect.name.lower():
        case "mysql":
            return SqlaMySqlClient(engine)
        case "postgresql":
            return SqlaPostgresClient(engine)
        case _:
            raise FeatureNotSupportedException
