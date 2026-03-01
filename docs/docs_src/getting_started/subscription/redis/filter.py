from faststream import FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)

subscriber = broker.subscriber("test-channel")

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
        channel="test-channel",
    )

    await broker.publish(
        "Hello, FastStream!",
        channel="test-channel",
    )
