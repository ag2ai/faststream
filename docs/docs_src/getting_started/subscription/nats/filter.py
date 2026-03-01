from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)

subscriber = broker.subscriber("test-subject")

@subscriber(
    filter=lambda msg: msg.content_type == "application/json",
)
async def handle(name: str, user_id: int):
    assert name == "John"
    assert user_id == 1

@subscriber
async def default_handler(msg: str):
    assert msg == "Hello, FastStream!"



@app.after_startup
async def test():
    await broker.publish(
        {"name": "John", "user_id": 1},
        subject="test-subject",
    )

    await broker.publish(
        "Hello, FastStream!",
        subject="test-subject",
    )
