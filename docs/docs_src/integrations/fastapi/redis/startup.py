from fastapi import FastAPI

from faststream.redis.fastapi import RedisRouter

router = RedisRouter("redis://localhost:6379")


@router.subscriber("test")
async def hello(msg: str):
    return {"response": "Hello, Redis!"}


@router.after_startup
async def test(app: FastAPI):
    await router.broker.publish("Hello!", "test")


app = FastAPI()
app.include_router(router)
