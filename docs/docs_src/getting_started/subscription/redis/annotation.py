from faststream import FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


@broker.subscriber("test-channel")
async def handle(
    name: str,
    user_id: int,
):
    assert name == "John"
    assert user_id == 1
