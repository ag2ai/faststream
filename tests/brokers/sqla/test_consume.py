import asyncio
from datetime import datetime, timedelta, timezone
import json
from typing import Any
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream.sqla.message import SqlaMessageState
from faststream.sqla.retry import ConstantRetryStrategy, NoRetryStrategy
from tests.brokers.sqla.basic import SqlaTestcaseConfig


# @pytest.mark.sqla()
@pytest.mark.connected()
class TestConsume(SqlaTestcaseConfig):
    @pytest.mark.asyncio()
    async def test_consume(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        attempted = []

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            graceful_shutdown_timeout=10,
            release_stuck_timeout=10,
        )
        async def handler(msg: Any) -> None:
            nonlocal attempted
            attempted.append(msg)

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        assert len(attempted) == 1

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().one()
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == SqlaMessageState.COMPLETED.name
        assert result["attempts_count"] == 1
        assert result["created_at"] < datetime.now(tz=timezone.utc)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]
        assert result["archived_at"] < datetime.now(tz=timezone.utc) and result["archived_at"] > result["first_attempt_at"]
    
    @pytest.mark.asyncio()
    async def test_consume_retry_allowed(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(delay_seconds=10, max_total_delay_seconds=None, max_attempts=None),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            graceful_shutdown_timeout=10,
            release_stuck_timeout=10,
        )
        async def handler(msg: Any) -> None:
            return 1/0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        result = result.mappings().one()
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == SqlaMessageState.RETRYABLE.name
        assert result["attempts_count"] == 1
        assert result["created_at"] < datetime.now(tz=timezone.utc)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]

        assert result["next_attempt_at"] > datetime.now(tz=timezone.utc) + timedelta(seconds=5)
        assert result["acquired_at"] == None
    
    @pytest.mark.asyncio()
    @pytest.mark.parametrize("end_state", [SqlaMessageState.COMPLETED, SqlaMessageState.FAILED])
    async def test_consume_retry_not_allowed(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event, end_state: SqlaMessageState) -> None:
        broker = self.get_broker(engine=engine)

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            graceful_shutdown_timeout=10,
            release_stuck_timeout=10,
        )
        async def handler(msg: Any) -> None:
            if end_state == SqlaMessageState.FAILED:
                return 1/0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().one()
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == end_state.name
        assert result["attempts_count"] == 1
        assert result["created_at"] < datetime.now(tz=timezone.utc)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]
        
        assert result["archived_at"] < datetime.now(tz=timezone.utc) and result["archived_at"] > result["first_attempt_at"]
    
    @pytest.mark.asyncio()
    async def test_consume_retry_not_allowed_prior_to_attempt(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        attempted = []

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            graceful_shutdown_timeout=10,
            release_stuck_timeout=10,
        )
        async def handler(msg: Any) -> None:
            nonlocal attempted
            attempted.append(msg)

        await broker.publish({"message": "hello1"}, queue="default1")
        async with engine.begin() as conn:
            result = await conn.execute(
                text(
                    "UPDATE message "
                    "SET attempts_count = 1, first_attempt_at = NOW() "
                    "WHERE id = 1;"
                )
            )
        await broker.start()

        await asyncio.sleep(0.5)

        assert len(attempted) == 0

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().one()
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == SqlaMessageState.FAILED.name
        assert result["attempts_count"] == 2
        assert result["created_at"] < datetime.now(tz=timezone.utc)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] > result["first_attempt_at"]
        
        assert result["archived_at"] < datetime.now(tz=timezone.utc) and result["archived_at"] > result["first_attempt_at"]
    
    @pytest.mark.asyncio()
    async def test_consume_by_queues(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        messages = []

        @broker.subscriber(
            engine=engine,
            queues=["default1", "default2"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            graceful_shutdown_timeout=10,
            release_stuck_timeout=10,
        )
        async def handler(msg: Any) -> None:
            nonlocal messages
            messages.append(msg["message"])
            if msg["message"] == "hello2":
                event.set()

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello3"}, queue="default3")        
        await broker.publish({"message": "hello2"}, queue="default2")        
        await broker.start()

        await asyncio.wait_for(event.wait(), timeout=self.timeout)
        
        assert messages == ["hello1", "hello2"]
    
    @pytest.mark.asyncio()
    async def test_consume_by_next_attempt_at(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        messages = []

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=0.01,
            min_fetch_interval=0.01,
            fetch_batch_size=1,
            overfetch_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            graceful_shutdown_timeout=10,
            release_stuck_timeout=10,
        )
        async def handler(msg: Any) -> None:
            nonlocal messages
            messages.append(msg["message"])

        await broker.publish({"message": "hello1"}, queue="default1", next_attempt_at=datetime.now() - timedelta(seconds=10))
        await broker.publish({"message": "hello2"}, queue="default1", next_attempt_at=datetime.now() + timedelta(seconds=10))        
        await broker.publish({"message": "hello3"}, queue="default1", next_attempt_at=datetime.now() - timedelta(seconds=20))        
        await broker.start()

        await asyncio.sleep(0.5)
        
        assert messages == ["hello3", "hello1"]