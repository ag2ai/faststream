from faststream import FastStream
from faststream.redis import RedisBroker, RedisRouter

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)
router = RedisRouter(prefix="prefix_")


@router.subscriber("test-channel")
@router.publisher("another-channel")
async def handle(name: str, user_id: int) -> str:
    assert name == "John"
    assert user_id == 1
    return "Hi!"


@router.subscriber("another-channel")
async def handle_response(msg: str):
    assert msg == "Hi!"


broker.include_router(router)


@app.after_startup
async def test():
    await broker.publish(
        {"name": "John", "user_id": 1},
        channel="prefix_test-channel",
    )
