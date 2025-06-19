from faststream import FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


@broker.subscriber("test-channel")
async def handle(msg: str):
    print(msg)
    assert msg == "Hi!"


@app.after_startup
async def test():
    async with RedisBroker("redis://localhost:6379") as br:
        await br.publish("Hi!", channel="test-channel")
