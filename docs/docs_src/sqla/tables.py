from datetime import datetime, timezone
from sqlalchemy import MetaData, Table
from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Enum,
    LargeBinary,
    SmallInteger,
    String,
    Table,
)

from faststream.sqla.message import SqlaMessageState

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
