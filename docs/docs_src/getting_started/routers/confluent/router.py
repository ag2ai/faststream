from faststream import FastStream
from faststream.confluent import KafkaBroker, KafkaRouter

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)
router = KafkaRouter(prefix="prefix_")


@router.subscriber("test-topic")
@router.publisher("another-topic")
async def handle(name: str, user_id: int) -> str:
    assert name == "John"
    assert user_id == 1
    return "Hi!"


@router.subscriber("another-topic")
async def handle_response(msg: str):
    assert msg == "Hi!"


broker.include_router(router)


@app.after_startup
async def test():
    await broker.publish(
        {"name": "John", "user_id": 1},
        topic="prefix_test-topic",
    )
