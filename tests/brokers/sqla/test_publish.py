import datetime
import json

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream.sqla import SqlaBroker
from faststream.sqla.exceptions import DatetimeMissingTimezoneException
from tests.brokers.base.publish import BrokerPublishTestcase
from tests.brokers.sqla.helpers import as_datetime

from .basic import SqlaTestcaseConfig


@pytest.mark.sqla()
@pytest.mark.connected()
@pytest.mark.slow()
class TestPublish(SqlaTestcaseConfig, BrokerPublishTestcase):
    @pytest.mark.asyncio()
    async def test_reply_to(self) -> None: ...

    @pytest.mark.asyncio()
    async def test_no_reply(self) -> None: ...

    test_reusable_publishers = pytest.mark.flaky(reruns=3, reruns_delay=1)(
        BrokerPublishTestcase.test_reusable_publishers
    )

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_with_next_attempt_at_without_timezone(
        self, mode: str, broker: SqlaBroker
    ) -> None:
        publisher = broker.publisher("default1")

        with pytest.raises(DatetimeMissingTimezoneException):  # noqa: PT012
            match mode:
                case "publish":
                    await broker.publish(
                        {"message": "hello1"},
                        queue="default1",
                        next_attempt_at=datetime.datetime.now(),  # noqa: DTZ005
                    )
                case "publisher":
                    await publisher.publish(
                        {"message": "hello1"},
                        next_attempt_at=datetime.datetime.now(),  # noqa: DTZ005
                    )

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_with_next_attempt_at_converts_timezone_to_utc(
        self, engine: AsyncEngine, mode: str, broker: SqlaBroker
    ) -> None:
        publisher = broker.publisher("default1")

        match mode:
            case "publish":
                await broker.publish(
                    {"message": "hello1"},
                    queue="default1",
                    next_attempt_at=datetime.datetime(
                        year=2026,
                        month=1,
                        day=1,
                        hour=12,
                        minute=0,
                        second=0,
                        tzinfo=datetime.timezone(datetime.timedelta(hours=3), "MSC"),
                    ),
                )
            case "publisher":
                await publisher.publish(
                    {"message": "hello1"},
                    next_attempt_at=datetime.datetime(
                        year=2026,
                        month=1,
                        day=1,
                        hour=12,
                        minute=0,
                        second=0,
                        tzinfo=datetime.timezone(datetime.timedelta(hours=3), "MSC"),
                    ),
                )

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        result = result.mappings().all()

        assert as_datetime(result[0]["next_attempt_at"]) == datetime.datetime(  # noqa: DTZ001
            year=2026,
            month=1,
            day=1,
            hour=9,
            minute=0,
            second=0,
        )


@pytest.mark.sqla()
@pytest.mark.connected()
class TestPublishTransaction(SqlaTestcaseConfig):
    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_wo_transaction(
        self, engine: AsyncEngine, mode: str, broker: SqlaBroker
    ) -> None:
        publisher = broker.publisher("default1")

        match mode:
            case "publish":
                await broker.publish({"message": "hello1"}, queue="default1")
            case "publisher":
                await publisher.publish({"message": "hello1"})

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 1

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_in_transaction(
        self, engine: AsyncEngine, mode: str, broker: SqlaBroker
    ) -> None:
        publisher = broker.publisher("default1")

        async with engine.begin() as conn:
            match mode:
                case "publish":
                    await broker.publish(
                        {"message": "hello1"}, queue="default1", connection=conn
                    )
                case "publisher":
                    await publisher.publish({"message": "hello1"}, connection=conn)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 1

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_in_transaction_rollback(
        self, engine: AsyncEngine, mode: str, broker: SqlaBroker
    ) -> None:
        publisher = broker.publisher("default1")

        async with engine.begin() as conn:
            match mode:
                case "publish":
                    await broker.publish(
                        {"message": "hello1"}, queue="default1", connection=conn
                    )
                case "publisher":
                    await publisher.publish({"message": "hello1"}, connection=conn)
            await conn.rollback()

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        assert len(result.all()) == 0


@pytest.mark.sqla()
@pytest.mark.connected()
@pytest.mark.slow()
class TestPublishBatch(SqlaTestcaseConfig):
    @pytest.mark.asyncio()
    async def test_publish_batch_inserts_all_messages(
        self, engine: AsyncEngine, broker: SqlaBroker
    ) -> None:
        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            {"message": "hello3"},
            queue="batch-queue",
        )

        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT queue, payload FROM message ORDER BY id")
            )
            rows = result.all()

        assert len(rows) == 3
        assert all(row.queue == "batch-queue" for row in rows)
        payloads = [json.loads(bytes(row.payload).decode()) for row in rows]
        assert payloads == [
            {"message": "hello1"},
            {"message": "hello2"},
            {"message": "hello3"},
        ]

    @pytest.mark.asyncio()
    async def test_publish_batch_uses_single_sql_statement(
        self, engine: AsyncEngine, broker: SqlaBroker
    ) -> None:
        inserts: list[str] = []

        def _capture(conn, cursor, statement, parameters, context, executemany) -> None:
            if statement.lstrip().upper().startswith("INSERT INTO MESSAGE"):
                inserts.append(statement)

        event.listen(engine.sync_engine, "before_cursor_execute", _capture)
        try:
            await broker.publish_batch(
                b"a",
                b"b",
                b"c",
                b"d",
                queue="batch-queue",
            )
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", _capture)

        assert len(inserts) == 1

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM message"))
            assert result.scalar() == 4

    @pytest.mark.asyncio()
    async def test_publish_batch_empty_is_noop(
        self, engine: AsyncEngine, broker: SqlaBroker
    ) -> None:
        await broker.publish_batch(queue="batch-queue")

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM message"))
            assert result.scalar() == 0

    @pytest.mark.asyncio()
    async def test_publish_batch_stores_headers_per_message(
        self, engine: AsyncEngine, broker: SqlaBroker
    ) -> None:
        await broker.publish_batch(
            {"message": "hello1"},
            b"raw",
            queue="batch-queue",
            headers={"x-custom": "value"},
        )

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT headers FROM message ORDER BY id"))
            rows = result.all()

        headers = [
            row.headers if isinstance(row.headers, dict) else json.loads(row.headers)
            for row in rows
        ]
        assert headers[0] == {
            "content-type": "application/json",
            "x-custom": "value",
        }
        assert headers[1] == {"x-custom": "value"}

    @pytest.mark.asyncio()
    async def test_publish_batch_with_next_attempt_at(
        self, engine: AsyncEngine, broker: SqlaBroker
    ) -> None:
        next_attempt_at = datetime.datetime(
            year=2026,
            month=1,
            day=1,
            hour=12,
            minute=0,
            second=0,
            tzinfo=datetime.timezone(datetime.timedelta(hours=3), "MSC"),
        )

        await broker.publish_batch(
            {"message": "hello1"},
            {"message": "hello2"},
            queue="batch-queue",
            next_attempt_at=next_attempt_at,
        )

        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT next_attempt_at FROM message ORDER BY id")
            )
            rows = result.all()

        expected = datetime.datetime(  # noqa: DTZ001
            year=2026, month=1, day=1, hour=9, minute=0, second=0
        )
        for row in rows:
            assert as_datetime(row.next_attempt_at) == expected

    @pytest.mark.asyncio()
    async def test_publish_batch_without_timezone_raises(
        self, broker: SqlaBroker
    ) -> None:
        with pytest.raises(DatetimeMissingTimezoneException):
            await broker.publish_batch(
                {"message": "hello1"},
                {"message": "hello2"},
                queue="batch-queue",
                next_attempt_at=datetime.datetime.now(),  # noqa: DTZ005
            )


@pytest.mark.sqla()
@pytest.mark.connected()
class TestPublishBatchTransaction(SqlaTestcaseConfig):
    @pytest.mark.asyncio()
    async def test_publish_batch_in_transaction(
        self, engine: AsyncEngine, broker: SqlaBroker
    ) -> None:
        async with engine.begin() as conn:
            await broker.publish_batch(
                b"a",
                b"b",
                queue="batch-queue",
                connection=conn,
            )

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM message"))
        assert result.scalar() == 2

    @pytest.mark.asyncio()
    async def test_publish_batch_in_transaction_rollback(
        self, engine: AsyncEngine, broker: SqlaBroker
    ) -> None:
        async with engine.begin() as conn:
            await broker.publish_batch(
                b"a",
                b"b",
                queue="batch-queue",
                connection=conn,
            )
            await conn.rollback()

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM message"))
        assert result.scalar() == 0
