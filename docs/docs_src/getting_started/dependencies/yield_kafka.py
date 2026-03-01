from contextlib import contextmanager

from faststream import Depends, FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@contextmanager
def fake_session_builder():
    yield "session"


async def db_session():
    with fake_session_builder() as session:
        yield session


@broker.subscriber("test-topic")
async def handle(db_session=Depends(db_session)):
    assert db_session == "session"


@app.after_startup
async def test():
    await broker.publish("", topic="test-topic")
