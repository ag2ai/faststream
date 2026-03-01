from faststream import FastStream
from faststream.nats import NatsBroker
from faststream.nats import NatsRoute, NatsRouter, NatsPublisher

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)


async def handle(name: str, user_id: int):
    assert name == "John"
    assert user_id == 1
    return "Hi!"


router = NatsRouter(
    handlers=(
        NatsRoute(
            handle,
            "test-subject",
            publishers=(
                NatsPublisher("outer-subject"),
            ),
        ),
    ),
)

broker.include_router(router)


@app.after_startup
async def test():
    await broker.publish({"name": "John", "user_id": 1}, subject="test-subject")
