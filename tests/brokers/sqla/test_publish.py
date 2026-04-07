import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream.sqla import SqlaBroker
from faststream.sqla.exceptions import DatetimeMissingTimezoneException
from tests.brokers.base.publish import BrokerPublishTestcase
from tests.brokers.sqla.conftest import as_datetime

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
