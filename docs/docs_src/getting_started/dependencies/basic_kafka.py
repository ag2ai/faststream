from faststream import Depends, FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


async def get_user_from_db(name: str, user_id: int):
    """Emulate DB request"""
    return {
        "name": name,
        "user_id": user_id,
    }


@broker.subscriber("test-topic")
async def handle(user=Depends(get_user_from_db)):
    assert user == {
        "name": "John",
        "user_id": 1,
    }


@app.after_startup
async def test():
    await broker.publish({"name": "John", "user_id": 1}, topic="test-topic")
