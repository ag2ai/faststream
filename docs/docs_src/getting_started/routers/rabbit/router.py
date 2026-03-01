from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitRouter

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)
router = RabbitRouter(prefix="prefix_")


@router.subscriber("test-queue")
@router.publisher("another-queue")
async def handle(name: str, user_id: int) -> str:
    assert name == "John"
    assert user_id == 1
    return "Hi!"


@router.subscriber("another-queue")
async def handle_response(msg: str):
    assert msg == "Hi!"


broker.include_router(router)


@app.after_startup
async def test():
    await broker.publish(
        {"name": "John", "user_id": 1},
        queue="prefix_test-queue",
    )
