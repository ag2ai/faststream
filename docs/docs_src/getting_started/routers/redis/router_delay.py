from faststream import FastStream
from faststream.redis import RedisBroker
from faststream.redis import RedisRouter, RedisRoute, RedisPublisher

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


async def handle(name: str, user_id: int):
    assert name == "John"
    assert user_id == 1
    return "Hi!"


router = RedisRouter(
    handlers=(
        RedisRoute(
            handle,
            "test-channel",
            publishers=(
                RedisPublisher("outer-channel"),
            ),
        ),
    ),
)

broker.include_router(router)


@app.after_startup
async def test():
    await broker.publish({"name": "John", "user_id": 1}, channel="test-channel")
