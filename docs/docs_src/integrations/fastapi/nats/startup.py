from fastapi import FastAPI

from faststream.nats.fastapi import NatsRouter

router = NatsRouter("nats://localhost:4222")


@router.subscriber("test")
async def hello(msg: str):
    return {"response": "Hello, NATS!"}


@router.after_startup
async def test(app: FastAPI):
    await router.broker.publish("Hello!", "test")


app = FastAPI()
app.include_router(router)
