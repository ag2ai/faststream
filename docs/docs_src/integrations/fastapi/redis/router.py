from fastapi import FastAPI
from faststream.redis import RedisRouter
from faststream.redis.fastapi import RedisRouter as StreamRouter

core_router = StreamRouter()
nested_router = RedisRouter()

@core_router.subscriber("core-channel")
async def handler():
    ...

@nested_router.subscriber("nested-channel")
async def nested_handler():
    ...

core_router.include_router(nested_router)

app = FastAPI()
app.include_router(core_router)
