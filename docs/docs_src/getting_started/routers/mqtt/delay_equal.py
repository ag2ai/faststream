from faststream import FastStream
from faststream.mqtt import MQTTBroker
from faststream.mqtt import MQTTRouter, MQTTRoute, MQTTPublisher

broker = MQTTBroker("localhost", port=1883)
app = FastStream(broker)

router = MQTTRouter()

@router.subscriber("test-topic")
@router.publisher("outer-topic")
async def handle(name: str, user_id: int):
    assert name == "John"
    assert user_id == 1
    return "Hi!"

broker.include_router(router)

@app.after_startup
async def test():
    await broker.publish({"name": "John", "user_id": 1}, topic="test-topic")
