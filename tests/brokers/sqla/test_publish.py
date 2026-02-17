import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import SqlaTestcaseConfig


@pytest.mark.sqla()
@pytest.mark.connected()
@pytest.mark.slow()
class TestPublish(SqlaTestcaseConfig, BrokerPublishTestcase):
    @pytest.mark.asyncio()
    async def test_reply_to(self) -> None: ...

    @pytest.mark.asyncio()
    async def test_no_reply(self) -> None: ...


@pytest.mark.sqla()
@pytest.mark.connected()
class TestPublishTransaction(SqlaTestcaseConfig):
    @pytest.mark.asyncio()
    @pytest.mark.parametrize("mode", ("publish", "publisher"))
    async def test_publish_wo_transaction(self, engine: AsyncEngine, mode: str) -> None:
        broker = self.get_broker(engine=engine)
        publisher = broker.publisher("default1")

        await broker.connect()

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
    async def test_publish_in_transaction(self, engine: AsyncEngine, mode: str) -> None:
        broker = self.get_broker(engine=engine)
        publisher = broker.publisher("default1")

        await broker.connect()

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
        self, engine: AsyncEngine, mode: str
    ) -> None:
        broker = self.get_broker(engine=engine)
        publisher = broker.publisher("default1")

        await broker.connect()

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
