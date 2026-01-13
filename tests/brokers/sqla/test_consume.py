import asyncio
import json
from datetime import datetime, timedelta, timezone
import logging
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream._internal.context import ContextRepo
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqla.message import SqlaInnerMessage, SqlaMessageState, SqlaMessage
from faststream.sqla.annotations import SqlaMessage as SqlaMessageAnnotation
from faststream.sqla.annotations import SqlaBroker as SqlaBrokerAnnotation
from faststream.sqla.annotations import ContextRepo as ContextRepoAnnotation
from faststream.sqla.annotations import Logger as LoggerAnnotation
from faststream.sqla.broker.broker import SqlaBroker as SqlaBroker
from faststream.sqla.retry import ConstantRetryStrategy, NoRetryStrategy
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.brokers.sqla.basic import SqlaTestcaseConfig


@pytest.mark.sqla()
@pytest.mark.connected()
@pytest.mark.slow()
class TestConsume(SqlaTestcaseConfig, BrokerRealConsumeTestcase):
    async def test_get_one_conflicts_with_handler(self) -> None:
        ...
    
    async def test_get_one(self) -> None:
        ...
    
    async def test_get_one_timeout(self) -> None:
        ...
    
    async def test_iteration(self) -> None:
        ...
        
    @pytest.mark.asyncio()
    async def test_consume(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        """
        Message was processed and archived.
        """
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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
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
        assert result["deliveries_count"] == 1
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]
        assert result["archived_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["archived_at"] > result["first_attempt_at"]

    @pytest.mark.asyncio()
    async def test_consume_nack_retry(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        """
        On exception message was marked as retryable with next attempts scheduled.
        """
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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
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
        assert result["deliveries_count"] == 1
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None).replace(tzinfo=None) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]

        assert result["next_attempt_at"] >= result["first_attempt_at"] + timedelta(seconds=10)
        assert result["acquired_at"] == None
    
    @pytest.mark.asyncio()
    async def test_consume_nack_no_retry(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        """
        On exception message was marked as failed and was archived.
        """
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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            return 1/0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().one()
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == SqlaMessageState.FAILED.name
        assert result["attempts_count"] == 1
        assert result["deliveries_count"] == 1
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]
        
        assert result["archived_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["archived_at"] > result["first_attempt_at"]
    
    @pytest.mark.asyncio()
    @pytest.mark.parametrize("max_deliveries", [1, None])
    async def test_consume_max_deliveries(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event, max_deliveries: int | None) -> None:
        """
        Message that was attempted but got stuck was not allowed a retry due to
        reached delivery limit.
        """
        logger = MagicMock()
        broker = self.get_broker(engine=engine, logger=logger, graceful_timeout=0.1)

        attempted = []

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(delay_seconds=0, max_total_delay_seconds=None, max_attempts=None),
            max_fetch_interval=0.1,
            min_fetch_interval=0.1,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=1,
            max_deliveries=max_deliveries,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            nonlocal attempted
            attempted.append(msg)
            await asyncio.sleep(1)

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()
        await asyncio.sleep(0.5)
        # stop with short graceful_shutdown_timeout so that message becomes stuck
        await broker.stop()
        assert len(attempted) == 1
        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        
        result = result.mappings().one()
        assert result["state"] == SqlaMessageState.PROCESSING.name
        assert result["attempts_count"] == 0
        assert result["deliveries_count"] == 1
        
        await broker.start()
        await asyncio.sleep(0.5)

        if max_deliveries:
            assert len(attempted) == 1
            
            async with engine.begin() as conn:
                result = await conn.execute(text("SELECT * FROM message_archive;"))
            
            result = result.mappings().one()
            assert result["queue"] == "default1"
            assert json.loads(result["payload"]) == {"message": "hello1"}
            assert result["state"] == SqlaMessageState.FAILED.name
            assert result["attempts_count"] == 0
            assert result["deliveries_count"] == 2
            assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
            assert result["first_attempt_at"] == None
            assert result["last_attempt_at"] == None
            
            assert result["archived_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)

            logs = [x for x in logger.log.call_args_list if x[0][0] == logging.ERROR]
            assert len(logs) == 2
            assert "Message delivery limit was exceeded for message" in logs[-1][0][1]
        else:
            assert len(attempted) == 2

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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
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
        assert result["deliveries_count"] == 3
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] > result["first_attempt_at"] and result["last_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)

    @pytest.mark.asyncio()
    async def test_consume_no_retry_strategy(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        """
        On exception message was marked as failed and was archived.
        """
        broker = self.get_broker(engine=engine)

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=1,
            retry_strategy=None,
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            return 1/0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().one()
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == SqlaMessageState.FAILED.name
        assert result["attempts_count"] == 1
        assert result["deliveries_count"] == 1
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]
        
        assert result["archived_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result["archived_at"] > result["first_attempt_at"]

    @pytest.mark.asyncio()
    async def test_consume_by_queues(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        """
        Messages from the specified queues were consumed.
        """
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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
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
    async def test_consume_current_messages_are_flushed_on_stop(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        """
        Processing of attempted messages completed and results were flushed.
        Acquired but not attemted messages were requeued.
        """
        broker = self.get_broker(engine=engine, graceful_timeout=2)

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=2,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=4,
            overfetch_factor=1,
            flush_interval=0.01,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            event.set()
            await asyncio.sleep(1)

        # attempted
        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")
        # not attempted
        await broker.publish({"message": "hello3"}, queue="default1")
        await broker.publish({"message": "hello4"}, queue="default1")
        
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)
        await broker.stop()
        
        async with engine.begin() as conn:
            result_1 = await conn.execute(text("SELECT * FROM message_archive;"))
            result_2 = await conn.execute(text("SELECT * FROM message;"))
        
        result_1 = result_1.mappings().all()
        assert len(result_1) == 2
        assert result_1[0]["state"] == SqlaMessageState.COMPLETED.name
        assert result_1[0]["attempts_count"] == 1
        assert result_1[0]["deliveries_count"] == 1
        assert result_1[0]["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result_1[0]["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result_1[0]["first_attempt_at"] > result_1[0]["created_at"]
        assert result_1[0]["last_attempt_at"] == result_1[0]["first_attempt_at"]
        assert result_1[0]["archived_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None) and result_1[0]["archived_at"] > result_1[0]["first_attempt_at"]

        result_2 = result_2.mappings().all()
        assert len(result_2) == 2
        assert result_2[0]["state"] == SqlaMessageState.PENDING.name
        assert result_2[0]["attempts_count"] == 0
        assert result_2[0]["deliveries_count"] == 0
        assert result_2[0]["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result_2[0]["acquired_at"] == None
        assert result_2[0]["first_attempt_at"] == None
        assert result_2[0]["next_attempt_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result_2[0]["last_attempt_at"] == None

    @pytest.mark.asyncio()
    async def test_consume_manual_ack_takes_precedence(self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event) -> None:
        """
        Manual Ack overrode automatic Reject.
        """
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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
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
        """
        Manual Nack overrode automatic Ack.
        """
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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
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
        """
        Manual Reject overrode automatic Ack.
        """
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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
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

        body_ = None
        message_ = None
        broker_ = None
        context_ = None
        logger_ = None

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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(
            body: Any,
            message: SqlaMessageAnnotation,
            broker: SqlaBrokerAnnotation,
            context: ContextRepoAnnotation,
            logger: LoggerAnnotation
        ) -> None:
            nonlocal body_
            nonlocal message_
            nonlocal broker_
            nonlocal context_
            nonlocal logger_
            body_ = body
            message_ = message
            broker_ = broker
            context_ = context
            logger_ = logger
            event.set()

        await broker.publish({"message": "hello1"}, queue="default1", headers={"header_1": "value_1"})
        await broker.start()
        await asyncio.wait_for(event.wait(), timeout=self.timeout)
        
        assert body_ == {"message": "hello1"}
        assert isinstance(message_, SqlaMessage)
        assert message_.headers == {'content-type': 'application/json', "header_1": "value_1"}
        assert isinstance(broker_, SqlaBroker)
        assert isinstance(context_, ContextRepo)
        assert isinstance(logger_, logging.Logger)
    
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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
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
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
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
        """
        Broker was stopped mid-processing, processing wasn't finalized/flushed,
        messages were requeued on next startup.
        """
        broker = self.get_broker(engine=engine, graceful_timeout=0.1)

        attempted = []

        @broker.subscriber(
            engine=engine,
            queues=["default1"],
            max_workers=2,
            retry_strategy=ConstantRetryStrategy(delay_seconds=0, max_total_delay_seconds=None, max_attempts=2),
            max_fetch_interval=0,
            min_fetch_interval=0,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=10,
            release_stuck_interval=10,
            release_stuck_timeout=0.5,
            max_deliveries=20,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            nonlocal attempted
            attempted.append(msg)
            await asyncio.sleep(1)

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.publish({"message": "hello2"}, queue="default1")

        # message is attempted but not finalized and not flushed
        await broker.start()
        await asyncio.sleep(0.5)
        await broker.stop()
        
        assert len(attempted) == 2
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message;"))
        result = result.mappings().all()
        assert len(result) == 2
        assert result[0]["state"] == SqlaMessageState.PROCESSING.name
        assert result[0]["attempts_count"] == 0
        assert result[0]["deliveries_count"] == 1

        await broker.start()
        await asyncio.sleep(0.1)

        assert len(attempted) == 4
    

