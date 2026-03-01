from faststream import FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)

publisher = broker.publisher("another-channel")

@broker.subscriber("test-channel")
async def handle():
    await publisher.publish("Hi!")


@broker.subscriber("another-channel")
async def handle_next(msg: str):
    assert msg == "Hi!"
