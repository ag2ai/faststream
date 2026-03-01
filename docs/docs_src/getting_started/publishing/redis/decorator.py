from faststream import FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


@broker.subscriber("test-channel")
@broker.publisher("another-channel")
async def handle() -> str:
    return "Hi!"


@broker.subscriber("another-channel")
async def handle_next(msg: str):
    assert msg == "Hi!"


@app.after_startup
async def test():
    await broker.publish("", channel="test-channel")
