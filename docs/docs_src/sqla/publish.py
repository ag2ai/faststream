from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import create_async_engine

from faststream import FastStream
from faststream.sqla import SqlaBroker

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker = SqlaBroker(engine=engine)
app = FastStream(broker)

publisher_sqla = broker.publisher()

@app.after_startup
async def publish_examples():
    await publisher_sqla.publish("Hello, SQLA!", queue="my_queue")

    await publisher_sqla.publish(
        "Process me later",
        queue="my_queue",
        next_attempt_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )

    async with engine.begin() as connection:
        # ... your other database operations using `connection` ...
        await publisher_sqla.publish(
            "Transactional message",
            queue="my_queue",
            connection=connection,
        )
