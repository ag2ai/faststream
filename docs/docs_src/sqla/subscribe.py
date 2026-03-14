from sqlalchemy.ext.asyncio import create_async_engine

from faststream import FastStream
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqla import SqlaBroker
from faststream.sqla.retry import ConstantRetryStrategy

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker = SqlaBroker(engine=engine)
app = FastStream(broker)


@broker.subscriber(
    queues=["my_queue"],
    max_workers=10,
    retry_strategy=ConstantRetryStrategy(
        delay_seconds=5,
        max_attempts=3,
        max_total_delay_seconds=None,
    ),
    min_fetch_interval=0.1,
    max_fetch_interval=1,
    fetch_batch_size=10,
    overfetch_factor=2,
    flush_interval=1,
    release_stuck_interval=60,
    release_stuck_timeout=60*5,
    max_deliveries=10,
    ack_policy=AckPolicy.NACK_ON_ERROR,
)
async def handler(msg: str):
    print(msg)
