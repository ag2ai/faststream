from faststream import FastStream
from faststream.nats import NatsBroker, NatsRouter

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)
router = NatsRouter(prefix="prefix_")


@router.subscriber("test-subject")
@router.publisher("another-subject")
async def handle(name: str, user_id: int) -> str:
    assert name == "John"
    assert user_id == 1
    return "Hi!"


@router.subscriber("another-subject")
async def handle_response(msg: str):
    assert msg == "Hi!"


broker.include_router(router)


@app.after_startup
async def test():
    await broker.publish(
        {"name": "John", "user_id": 1},
        subject="prefix_test-subject",
    )
