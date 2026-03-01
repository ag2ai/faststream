from faststream import FastStream
from faststream.nats import NatsBroker, NatsRouter

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)

router = NatsRouter()

@router.subscriber("test-subject")
@router.publisher("outer-subject")
async def handle(name: str, user_id: int):
    assert name == "John"
    assert user_id == 1
    return "Hi!"

broker.include_router(router)

@app.after_startup
async def test():
    await broker.publish({"name": "John", "user_id": 1}, subject="test-subject")
