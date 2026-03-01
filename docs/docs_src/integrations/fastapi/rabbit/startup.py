from fastapi import FastAPI

from faststream.rabbit.fastapi import RabbitRouter

router = RabbitRouter("amqp://guest:guest@localhost:5672/")


@router.subscriber("test")
async def hello(msg: str):
    return {"response": "Hello, Rabbit!"}


@router.after_startup
async def test(app: FastAPI):
    await router.broker.publish("Hello!", "test")


app = FastAPI()
app.include_router(router)
