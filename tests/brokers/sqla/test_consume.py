import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream.sqla.message import SqlaMessage, SqlaMessageState
from faststream.sqla.annotations import SqlaMessage as SqlaMessageAnnotation
from faststream.sqla.annotations import SqlaBroker as SqlaBrokerAnnotation
from faststream.sqla.broker.broker import SqlaBroker as SqlaBroker
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
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]
        assert result["archived_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["archived_at"] > result["first_attempt_at"]
    
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
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None).replace(tzinfo=None) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]

        assert result["next_attempt_at"] > datetime.now(tz=timezone.utc).replace(tzinfo=None).replace(tzinfo=None) + timedelta(seconds=5)
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
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]
        
        assert result["archived_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["archived_at"] > result["first_attempt_at"]
    
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
            match engine.dialect.name:
                case "postgresql":
                    stmt = text(
                        "UPDATE message "
                        "SET attempts_count = 1, first_attempt_at = NOW() "
                        "WHERE id = 1;"
                    )
                case "mysql":
                    stmt = text(
                        "UPDATE message "
                        "SET attempts_count = 1, first_attempt_at = UTC_TIMESTAMP(3) "
                        "WHERE id = 1;"
                    )
            result = await conn.execute(
                stmt
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
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] > result["first_attempt_at"]
        
        assert result["archived_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["archived_at"] > result["first_attempt_at"]
    
    @pytest.mark.asyncio()
    async def test_consume_full_retry_flow(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        attempted = []

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(delay_seconds=0.01, max_total_delay_seconds=None, max_attempts=3),
            max_fetch_interval=0.01,
            min_fetch_interval=0.01,
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
            return 1/0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        assert len(attempted) == 3

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().one()
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == SqlaMessageState.FAILED.name
        assert result["attempts_count"] == 3
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] > result["first_attempt_at"]

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

        await broker.publish({"message": "hello1"}, queue="default1", next_attempt_at=datetime.now(tz=timezone.utc) - timedelta(seconds=10))
        await broker.publish({"message": "hello2"}, queue="default1", next_attempt_at=datetime.now(tz=timezone.utc) + timedelta(seconds=10))        
        await broker.publish({"message": "hello3"}, queue="default1", next_attempt_at=datetime.now(tz=timezone.utc) - timedelta(seconds=20))        
        await broker.start()

        await asyncio.sleep(0.5)
        
        assert messages == ["hello3", "hello1"]
    
    @pytest.mark.asyncio()
    async def test_consume_stop_current_messages_are_flushed(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=2,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            graceful_shutdown_timeout=2,
            release_stuck_timeout=10,
        )
        async def handler(msg: Any) -> None:
            event.set()
            await asyncio.sleep(1)

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)
        await broker.stop()
        
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().all()
        assert len(result) == 2
        assert result[0]["state"] == SqlaMessageState.COMPLETED.name

    @pytest.mark.asyncio()
    async def test_consume_manual_ack_takes_precedence(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=2,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            graceful_shutdown_timeout=2,
            release_stuck_timeout=10,
        )
        async def handler(msg: SqlaMessageAnnotation, msg_body: dict) -> None:
            await msg.ack()
            return 1/0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)
        
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().all()
        assert len(result) == 2
        assert result[0]["state"] == SqlaMessageState.COMPLETED.name
        assert result[1]["state"] == SqlaMessageState.COMPLETED.name

    @pytest.mark.asyncio()
    async def test_consume_manual_nack_takes_precedence(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=2,
            retry_strategy=ConstantRetryStrategy(delay_seconds=0, max_total_delay_seconds=None, max_attempts=3),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            graceful_shutdown_timeout=2,
            release_stuck_timeout=10,
        )
        async def handler(msg: SqlaMessageAnnotation, msg_body: dict) -> None:
            await msg.nack()
            return

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)
        
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        result = result.mappings().all()
        assert len(result) == 2
        assert result[0]["state"] == SqlaMessageState.RETRYABLE.name
        assert result[1]["state"] == SqlaMessageState.RETRYABLE.name

    @pytest.mark.asyncio()
    async def test_consume_manual_reject_takes_precedence(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=2,
            retry_strategy=ConstantRetryStrategy(delay_seconds=0, max_total_delay_seconds=None, max_attempts=3),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            graceful_shutdown_timeout=2,
            release_stuck_timeout=10,
        )
        async def handler(msg: SqlaMessageAnnotation, msg_body: dict) -> None:
            await msg.reject()
            return

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)
        
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().all()
        assert len(result) == 2
        assert result[0]["state"] == SqlaMessageState.FAILED.name
        assert result[1]["state"] == SqlaMessageState.FAILED.name

    @pytest.mark.asyncio()
    async def test_consume_context_fields(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        message_ = None
        broker_ = None

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=2,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            graceful_shutdown_timeout=2,
            release_stuck_timeout=10,
        )
        async def handler(msg: SqlaMessageAnnotation, broker: SqlaBrokerAnnotation) -> None:
            nonlocal message_
            nonlocal broker_
            message_ = msg
            broker_ = broker
            event.set()

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)
        assert isinstance(message_, SqlaMessage)
        assert isinstance(broker_, SqlaBroker)
    
    @pytest.mark.asyncio()
    async def test_consume_concurrency(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        attempted = []

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=4,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=0,
            fetch_batch_size=4,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            graceful_shutdown_timeout=10,
            release_stuck_timeout=10,
        )
        async def handler(msg: Any) -> None:
            await asyncio.sleep(1)
            nonlocal attempted
            attempted.append(msg)

        for idx in range(8):
            await broker.publish({"message": f"hello{idx+1}"}, queue="default1")
        await broker.start()

        await asyncio.sleep(1.5)
        assert len(attempted) == 4

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive WHERE state = 'COMPLETED';"))
        result = result.mappings().all()
        assert len(result) == 4

        await asyncio.sleep(1)
        assert len(attempted) == 8

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive WHERE state = 'COMPLETED';"))
        result = result.mappings().all()
        assert len(result) == 8

    @pytest.mark.asyncio()
    async def test_consume_fetch_intervals(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        attempted = []

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=4,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=0,
            fetch_batch_size=4,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            graceful_shutdown_timeout=10,
            release_stuck_timeout=10,
        )
        async def handler(msg: Any) -> None:
            nonlocal attempted
            attempted.append(msg)
        
        for idx in range(7):
            await broker.publish({"message": f"hello{idx+1}"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)
        await broker.publish({"message": f"hello{idx+1}"}, queue="default1")
        await asyncio.sleep(0.1)

        assert len(attempted) == 7
    
    @pytest.mark.asyncio()
    async def test_consume_release_stuck(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        broker = self.get_broker(engine=engine)

        attempted = []

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(delay_seconds=0, max_total_delay_seconds=None, max_attempts=2),
            max_fetch_interval=0,
            min_fetch_interval=0,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            graceful_shutdown_timeout=10,
            release_stuck_timeout=60,
        )
        async def handler(msg: Any) -> None:
            nonlocal attempted
            attempted.append(msg)

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")
        async with engine.begin() as conn:
            match engine.dialect.name:
                case "postgresql":
                    stmt = text(
                        "UPDATE message "
                        "SET attempts_count = 1, "
                        "first_attempt_at = NOW() - INTERVAL '100 seconds', "
                        "acquired_at = NOW() - INTERVAL '100 seconds', "
                        "state = 'PROCESSING' "
                        "WHERE id = 1;"
                    )
                case "mysql":
                    stmt = text(
                        "UPDATE message "
                        "SET attempts_count = 1, "
                        "first_attempt_at = DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 100 SECOND), "
                        "acquired_at = DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 100 SECOND), "
                        "state = 'PROCESSING' "
                        "WHERE id = 1;"
                    )
            await conn.execute(
                stmt
            )

            match engine.dialect.name:
                case "postgresql":
                    stmt = text(
                        "UPDATE message "
                        "SET attempts_count = 1, "
                        "first_attempt_at = NOW() - INTERVAL '10 seconds', "
                        "acquired_at = NOW() - INTERVAL '10 seconds', "
                        "state = 'PROCESSING' "
                        "WHERE id = 2;"
                    )
                case "mysql":
                    stmt = text(
                        "UPDATE message "
                        "SET attempts_count = 1, "
                        "first_attempt_at = DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 10 SECOND), "
                        "acquired_at = DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 10 SECOND), "
                        "state = 'PROCESSING' "
                        "WHERE id = 2;"
                    )
            await conn.execute(
                stmt
            )

        await broker.start()

        await asyncio.sleep(0.5)

        assert len(attempted) == 1