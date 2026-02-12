import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqla.annotations import (
    SqlaMessage as SqlaMessageAnnotation,
)
from faststream.sqla.message import SqlaMessageState
from faststream.sqla.retry import ConstantRetryStrategy
from tests.brokers.sqla.basic import SqlaTestcaseConfig


@pytest.mark.sqla()
@pytest.mark.connected()
@pytest.mark.slow()
class TestConsumeAckPolicy(SqlaTestcaseConfig):
    @pytest.mark.asyncio()
    async def test_consume_nack_on_error(
        self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event
    ) -> None:
        """Message was Nack'ed."""
        broker = self.get_broker(engine=engine)

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=5, max_total_delay_seconds=None, max_attempts=None
            ),
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
            return 1 / 0

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
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(
            tzinfo=None
        ).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(
            tzinfo=None
        ).replace(tzinfo=None)
        assert result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]

        assert result["next_attempt_at"] >= result["first_attempt_at"] + timedelta(
            seconds=5
        )
        assert result["acquired_at"] is None

    @pytest.mark.asyncio()
    async def test_consume_reject_on_error(
        self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event
    ) -> None:
        """Message was Reject'ed despite the retry strategy."""
        broker = self.get_broker(engine=engine)

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=1, max_total_delay_seconds=None, max_attempts=3
            ),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )
        async def handler(msg: Any) -> None:
            return 1 / 0

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
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(
            tzinfo=None
        )
        assert result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]

        assert result["archived_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["archived_at"] > result["first_attempt_at"]

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("ack_policy", (AckPolicy.ACK_FIRST, AckPolicy.ACK))
    async def test_consume_ack_and_ack_first(
        self,
        engine: AsyncEngine,
        recreate_tables: None,
        event: asyncio.Event,
        ack_policy: AckPolicy,
    ) -> None:
        """Message was Ack'ed despite the error."""
        broker = self.get_broker(engine=engine)

        @broker.subscriber(
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
            ack_policy=ack_policy,
        )
        async def handler(msg: Any) -> None:
            return 1 / 0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().one()
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == SqlaMessageState.COMPLETED.name
        assert result["attempts_count"] == 1
        assert result["deliveries_count"] == 1
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(
            tzinfo=None
        )
        assert result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]

        assert result["archived_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["archived_at"] > result["first_attempt_at"]

    @pytest.mark.asyncio()
    async def test_consume_manual(
        self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event
    ) -> None:
        """Message was manually Ack'ed."""
        broker = self.get_broker(engine=engine)

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=5, max_total_delay_seconds=None, max_attempts=None
            ),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.MANUAL,
        )
        async def handler(msg: SqlaMessageAnnotation) -> None:
            await msg.ack()
            return 1 / 0

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT * FROM message_archive;"))
        result = result.mappings().one()
        assert result["queue"] == "default1"
        assert json.loads(result["payload"]) == {"message": "hello1"}
        assert result["state"] == SqlaMessageState.COMPLETED.name
        assert result["attempts_count"] == 1
        assert result["deliveries_count"] == 1
        assert result["created_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["first_attempt_at"] < datetime.now(tz=timezone.utc).replace(
            tzinfo=None
        )
        assert result["first_attempt_at"] > result["created_at"]
        assert result["last_attempt_at"] == result["first_attempt_at"]

        assert result["archived_at"] < datetime.now(tz=timezone.utc).replace(tzinfo=None)
        assert result["archived_at"] > result["first_attempt_at"]

    @pytest.mark.asyncio()
    async def test_consume_manual_no_manual_ack(
        self, engine: AsyncEngine, recreate_tables: None, event: asyncio.Event
    ) -> None:
        """Error was logged because the message was neither manually nor automatically acknowledged."""
        logger = MagicMock()
        broker = self.get_broker(engine=engine, logger=logger)

        @broker.subscriber(
            queues=["default1"],
            max_workers=1,
            retry_strategy=ConstantRetryStrategy(
                delay_seconds=5, max_total_delay_seconds=None, max_attempts=None
            ),
            max_fetch_interval=10,
            min_fetch_interval=10,
            fetch_batch_size=5,
            overfetch_factor=1,
            flush_interval=0.1,
            release_stuck_interval=10,
            release_stuck_timeout=10,
            max_deliveries=20,
            ack_policy=AckPolicy.MANUAL,
        )
        async def handler(msg: SqlaMessageAnnotation) -> None:
            return

        await broker.publish({"message": "hello1"}, queue="default1")
        await broker.start()

        await asyncio.sleep(0.5)

        logs = [x for x in logger.log.call_args_list if x[0][0] == logging.ERROR]
        assert len(logs) == 1
        assert "was not updated after processing" in logs[0][0][1]
