from sqlalchemy.ext.asyncio import create_async_engine
from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.sqla import SqlaBroker, SqlaMessage
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqla.retry import ExponentialBackoffRetryStrategy

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker_sqla = SqlaBroker(engine=engine)
broker_kafka = KafkaBroker("127.0.0.1:9092")
app = FastStream(broker_sqla, on_startup=[broker_kafka.connect])
publisher_sqla = broker_sqla.publisher()


@app.after_startup # just an example
async def publish_examples():
    async with engine.begin() as connection:
        # ... your other database operations using `connection` ...
        await publisher_sqla.publish(
            {"message": "Hello, SQLA!"},
            queue="sqla_queue",
            connection=connection,
        )


publisher_kafka = broker_kafka.publisher("kafka_topic")


@publisher_kafka
@broker_sqla.subscriber(
    queues=["sqla_queue"],
    max_workers=10,
    retry_strategy=ExponentialBackoffRetryStrategy(
        initial_delay_seconds=1,
        multiplier=2,
        max_delay_seconds=60 * 5,
        max_total_delay_seconds=60 * 60 * 6,
        max_attempts=None,
    ),
    max_fetch_interval=1,
    min_fetch_interval=0,
    fetch_batch_size=10,
    overfetch_factor=1.5,
    flush_interval=3,
    release_stuck_interval=5,
    release_stuck_timeout=60 * 60,
    max_deliveries=20,
    ack_policy=AckPolicy.NACK_ON_ERROR,
)
async def handle_msg(msg_body: dict) -> dict:
    return msg_body
