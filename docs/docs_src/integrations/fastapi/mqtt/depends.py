from fastapi import Depends, FastAPI
from typing_extensions import Annotated

from faststream.mqtt import MQTTBroker, fastapi

router = fastapi.MQTTRouter("localhost:1883")

app = FastAPI()


def broker():
    return router.broker


@router.get("/")
async def hello_http(broker: Annotated[MQTTBroker, Depends(broker)]):
    await broker.publish("Hello, MQTT!", "test")
    return "Hello, HTTP!"


app.include_router(router)
