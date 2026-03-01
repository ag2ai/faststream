from fastapi import Depends, FastAPI
from typing_extensions import Annotated

from faststream.nats import NatsBroker, fastapi

router = fastapi.NatsRouter("nats://localhost:4222")

app = FastAPI()


def broker():
    return router.broker


@router.get("/")
async def hello_http(broker: Annotated[NatsBroker, Depends(broker)]):
    await broker.publish("Hello, NATS!", "test")
    return "Hello, HTTP!"


app.include_router(router)
