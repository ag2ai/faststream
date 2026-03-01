from faststream import FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)

publisher = broker.publisher("another-channel")

@publisher
@broker.subscriber("test-channel")
async def handle() -> str:
    return "Hi!"


@broker.subscriber("another-channel")
async def handle_next(msg: str):
    assert msg == "Hi!"
