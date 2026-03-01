from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaRouter

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

router = KafkaRouter()

@router.subscriber("test-topic")
@router.publisher("outer-topic")
async def handle(name: str, user_id: int):
    assert name == "John"
    assert user_id == 1
    return "Hi!"

broker.include_router(router)

@app.after_startup
async def test():
    await broker.publish({"name": "John", "user_id": 1}, topic="test-topic")
