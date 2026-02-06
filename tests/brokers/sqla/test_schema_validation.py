import pytest
from sqlalchemy import (
    BigInteger,
    Column,
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
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream.exceptions import SetupError
from faststream.sqla.message import SqlaMessageState
from tests.brokers.sqla.basic import SqlaTestcaseConfig


@pytest.mark.sqla()
@pytest.mark.connected()
@pytest.mark.slow()
class TestSchemaValidation(SqlaTestcaseConfig):
    @pytest.mark.asyncio()
    async def test_schema_validation_passes(
        self, engine: AsyncEngine, recreate_tables: None
    ) -> None:
        broker = self.get_broker(engine=engine, validate_schema_on_start=True)

        await broker.start()
        await broker.stop()

    @pytest.mark.asyncio()
    async def test_schema_validation_disabled(self, engine: AsyncEngine) -> None:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS message_archive"))
            await conn.execute(text("DROP TABLE IF EXISTS message"))

        broker = self.get_broker(engine=engine, validate_schema_on_start=False)

        await broker.start()
        await broker.stop()

    @pytest.mark.asyncio()
    async def test_schema_validation_fails_missing_table(
        self, engine: AsyncEngine
    ) -> None:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS message_archive"))
            await conn.execute(text("DROP TABLE IF EXISTS message"))

        broker = self.get_broker(engine=engine, validate_schema_on_start=True)

        with pytest.raises(SetupError) as exc_info:
            await broker.start()

        assert "Table 'message' does not exist" in str(exc_info.value)

    @pytest.mark.asyncio()
    async def test_schema_validation_fails_missing_column(
        self, engine: AsyncEngine
    ) -> None:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS message_archive"))
            await conn.execute(text("DROP TABLE IF EXISTS message"))

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
        Table(
            "message",
            metadata,
            Column("id", BigInteger, primary_key=True),
            Column("queue", String(255), nullable=False),
            Column("headers", json_type, nullable=True),
            Column("payload", LargeBinary, nullable=False),
            Column("state", Enum(SqlaMessageState), nullable=False),
            Column("attempts_count", SmallInteger, nullable=False),
            Column("deliveries_count", SmallInteger, nullable=False),
            Column("created_at", timestamp_type, nullable=False),
            Column("first_attempt_at", timestamp_type),
            Column("next_attempt_at", timestamp_type, nullable=False),
            Column("last_attempt_at", timestamp_type),
            # missing: acquired_at # noqa: ERA001
        )
        Table(
            "message_archive",
            metadata,
            Column("id", BigInteger, primary_key=True),
            Column("queue", String(255), nullable=False),
            Column("headers", json_type, nullable=True),
            Column("payload", LargeBinary, nullable=False),
            Column("state", Enum(SqlaMessageState), nullable=False),
            Column("attempts_count", SmallInteger, nullable=False),
            Column("deliveries_count", SmallInteger, nullable=False),
            Column("created_at", timestamp_type, nullable=False),
            Column("first_attempt_at", timestamp_type),
            Column("last_attempt_at", timestamp_type),
            Column("archived_at", timestamp_type, nullable=False),
        )

        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

        broker = self.get_broker(engine=engine, validate_schema_on_start=True)

        with pytest.raises(SetupError) as exc_info:
            await broker.start()

        assert "missing columns" in str(exc_info.value)
        assert "acquired_at" in str(exc_info.value)

    @pytest.mark.asyncio()
    async def test_schema_validation_fails_wrong_column_type(
        self, engine: AsyncEngine
    ) -> None:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS message_archive"))
            await conn.execute(text("DROP TABLE IF EXISTS message"))

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
        Table(
            "message",
            metadata,
            Column("id", SmallInteger, primary_key=True),  # wrong: should be BigInteger
            Column("queue", Integer, nullable=False),  # wrong: should be String
            Column("headers", json_type, nullable=True),
            Column(
                "payload", String(255), nullable=False
            ),  # wrong: should be LargeBinary
            Column("state", Enum(SqlaMessageState), nullable=False),
            Column("attempts_count", SmallInteger, nullable=False),
            Column("deliveries_count", SmallInteger, nullable=False),
            Column(
                "created_at", String(255), nullable=False
            ),  # wrong: should be DateTime
            Column("first_attempt_at", timestamp_type),
            Column("next_attempt_at", timestamp_type, nullable=False),
            Column("last_attempt_at", timestamp_type),
            Column("acquired_at", timestamp_type),
        )
        Table(
            "message_archive",
            metadata,
            Column("id", BigInteger, primary_key=True),
            Column("queue", Integer, nullable=False),  # wrong: should be String
            Column("headers", String(255), nullable=True),  # wrong: should be JSON
            Column("payload", LargeBinary, nullable=False),
            Column("state", Enum(SqlaMessageState), nullable=False),
            Column("attempts_count", SmallInteger, nullable=False),
            Column("deliveries_count", SmallInteger, nullable=False),
            Column("created_at", timestamp_type, nullable=False),
            Column("first_attempt_at", timestamp_type),
            Column("last_attempt_at", timestamp_type),
            Column("archived_at", timestamp_type, nullable=False),
        )

        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

        broker = self.get_broker(engine=engine, validate_schema_on_start=True)

        with pytest.raises(SetupError) as exc_info:
            await broker.start()

        error_msg = str(exc_info.value)
        assert "message" in error_msg
        assert "id" in error_msg
        assert "queue" in error_msg
        assert "payload" in error_msg
        assert "created_at" in error_msg
        assert "message_archive" in error_msg
        assert "headers" in error_msg
