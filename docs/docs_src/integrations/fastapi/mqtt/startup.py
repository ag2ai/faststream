from fastapi import FastAPI

from faststream.mqtt.fastapi import MQTTRouter

router = MQTTRouter("localhost:1883")


@router.subscriber("test")
async def hello(msg: str):
    return {"response": "Hello, MQTT!"}


@router.after_startup
async def test(app: FastAPI):
    await router.broker.publish("Hello!", "test")


app = FastAPI()
app.include_router(router)
