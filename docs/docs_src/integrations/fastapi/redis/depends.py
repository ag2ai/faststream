from fastapi import Depends, FastAPI
from typing_extensions import Annotated

from faststream.redis import RedisBroker, fastapi

router = fastapi.RedisRouter("redis://localhost:6379")

app = FastAPI()


def broker():
    return router.broker


@router.get("/")
async def hello_http(broker: Annotated[RedisBroker, Depends(broker)]):
    await broker.publish("Hello, Redis!", "test")
    return "Hello, HTTP!"


app.include_router(router)
