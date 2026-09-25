from fastapi import FastAPI

from faststream.mqtt.fastapi import MQTTRouter

router = MQTTRouter("localhost:1883")

app = FastAPI()


@router.get("/")
async def hello_http():
    await router.broker.publish("Hello, MQTT!", "test")
    return "Hello, HTTP!"


app.include_router(router)
