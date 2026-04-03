import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream import AckPolicy
from faststream.sqla import SqlaBroker
from faststream.sqla.retry import NoRetryStrategy


@pytest_asyncio.fixture
async def broker(engine: AsyncEngine) -> SqlaBroker:
    return SqlaBroker(engine=engine)


@pytest.mark.sqla()
@pytest.mark.asyncio()
async def test_warn_on_max_deliveries(broker: SqlaBroker) -> None:
    with pytest.warns(
        UserWarning,
        match="max_deliveries violates the at most once processing guarantee",
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            overfetch_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            max_deliveries=3,
        )


@pytest.mark.sqla()
@pytest.mark.asyncio()
async def test_warn_when_retry_strategy_ignored(broker: SqlaBroker) -> None:
    with pytest.warns(
        UserWarning,
        match="retry_strategy is ignored when AckPolicy.REJECT_ON_ERROR is used",
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            overfetch_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )


@pytest.mark.sqla()
@pytest.mark.asyncio()
async def test_warn_when_nack_without_retry_strategy(broker: SqlaBroker) -> None:
    with pytest.warns(
        UserWarning,
        match="AckPolicy.NACK_ON_ERROR has the same effect as AckPolicy.REJECT_ON_ERROR for this broker",
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=None,
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            overfetch_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )


@pytest.mark.sqla()
@pytest.mark.asyncio()
async def test_warn_when_ack_first_used(broker: SqlaBroker) -> None:
    with pytest.warns(
        UserWarning,
        match="AckPolicy.ACK_FIRST has the same effect as AckPolicy.ACK for this broker",
    ):
        broker.subscriber(
            queues=["test"],
            max_workers=1,
            retry_strategy=NoRetryStrategy(),
            max_fetch_interval=1.0,
            min_fetch_interval=0.1,
            fetch_batch_size=10,
            overfetch_factor=1.0,
            flush_interval=0.1,
            release_stuck_interval=10.0,
            release_stuck_timeout=10.0,
            ack_policy=AckPolicy.ACK_FIRST,
        )
