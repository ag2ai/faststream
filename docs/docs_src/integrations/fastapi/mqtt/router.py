from fastapi import FastAPI
from faststream.mqtt import MQTTRouter
from faststream.mqtt.fastapi import MQTTRouter as StreamRouter

core_router = StreamRouter()
nested_router = MQTTRouter()

@core_router.subscriber("core-topic")
async def handler():
    ...

@nested_router.subscriber("nested-topic")
async def nested_handler():
    ...

core_router.include_router(nested_router)

app = FastAPI()
app.include_router(core_router)
