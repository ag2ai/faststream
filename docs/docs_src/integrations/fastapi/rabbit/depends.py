from fastapi import Depends, FastAPI
from typing_extensions import Annotated

from faststream.rabbit import RabbitBroker, fastapi

router = fastapi.RabbitRouter("amqp://guest:guest@localhost:5672/")

app = FastAPI()


def broker():
    return router.broker


@router.get("/")
async def hello_http(broker: Annotated[RabbitBroker, Depends(broker)]):
    await broker.publish("Hello, Rabbit!", "test")
    return "Hello, HTTP!"


app.include_router(router)
