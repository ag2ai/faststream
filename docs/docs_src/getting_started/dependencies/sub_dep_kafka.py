from faststream import Depends, FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


async def get_user_from_db(user_id: int):
    """Emulate DB request"""
    return {
        "name": "John",
        "user_id": user_id,
    }


async def validate_db_user(
    name: str,
    user=Depends(get_user_from_db),
):
    return user["name"] == name


@broker.subscriber("test-topic")
async def handle(is_user_valid=Depends(validate_db_user)):
    assert is_user_valid


@app.after_startup
async def test():
    await broker.publish({"name": "John", "user_id": 1}, topic="test-topic")
