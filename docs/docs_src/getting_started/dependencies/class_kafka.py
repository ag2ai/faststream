from faststream import Depends, FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


class UserSerialized:
    def __init__(self, name: str, user_id: int):
        self.name = name
        self.user_id = user_id


@broker.subscriber("test-topic")
async def handle(user=Depends(UserSerialized)):
    assert user.name == "John"
    assert user.user_id == 1


@app.after_startup
async def test():
    await broker.publish({"name": "John", "user_id": 1}, topic="test-topic")
