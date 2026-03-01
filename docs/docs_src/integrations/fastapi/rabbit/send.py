from fastapi import FastAPI

from faststream.rabbit.fastapi import RabbitRouter

router = RabbitRouter("amqp://guest:guest@localhost:5672/")

app = FastAPI()


@router.get("/")
async def hello_http():
    await router.broker.publish("Hello, Rabbit!", "test")
    return "Hello, HTTP!"


app.include_router(router)
